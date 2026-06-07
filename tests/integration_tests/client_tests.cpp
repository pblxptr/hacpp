#include "config.h"

#include <async_mqtt/protocol/packet/qos.hpp>
#include <hacpp/async_mqtt_client.h>
#include <hacpp/error.h>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/impl/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <catch2/catch_test_macros.hpp>
#include <spdlog/spdlog.h>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <string>
#include <utility>
#include <vector>

using hacpp::mqtt::Error;
using hacpp::mqtt::ErrorCode;

namespace {
boost::asio::awaitable<void> require_disconnected_publish(boost::asio::any_io_executor exe)
{
  auto client = hacpp::mqtt::AsyncMqttClient2{exe, config()};

  auto err = co_await client.async_publish("test/topic", "payload");

  REQUIRE(err);
  REQUIRE(err == ErrorCode::NotConnected);
  co_await client.async_close();
}

boost::asio::awaitable<void> require_disconnected_subscribe(boost::asio::any_io_executor exe)
{
  auto client = hacpp::mqtt::AsyncMqttClient2{exe, config()};
  auto topics = std::vector<hacpp::mqtt::TopicSubopts>{
      {"test/topic", async_mqtt::qos::at_most_once}
  };

  auto err = co_await client.async_subscribe(topics);

  REQUIRE(err);
  REQUIRE(err == ErrorCode::NotConnected);
  co_await client.async_close();
}
} // namespace

TEST_CASE("Client can connect to broker", "[client]")
{
  // Arrange
  auto io = boost::asio::io_context{};
  auto strand = boost::asio::make_strand(io);
  auto client = hacpp::mqtt::AsyncMqttClient2{strand, config()};

  // NOLINTBEGIN
  boost::asio::co_spawn(
      strand,
      [client = std::move(client)]() mutable -> boost::asio::awaitable<void> {
        // Act
        auto err = co_await client.async_connect();

        // Assert
        REQUIRE(!err);
        co_await client.async_close();
      },
      rethrow);
  // NOLINTEND

  io.run();
}

TEST_CASE("Client cannot connect to broker", "[client]")
{
  // Arrange
  auto io = boost::asio::io_context{};
  auto strand = boost::asio::make_strand(io);
  SECTION("when providing invalid credentials")
  {
    // Arrange
    auto invalid_config = config();
    invalid_config.password = "invalid_password";
    auto client = hacpp::mqtt::AsyncMqttClient2{strand, invalid_config};
    // NOLINTBEGIN
    boost::asio::co_spawn(
        strand,
        [client = std::move(client)]() mutable -> boost::asio::awaitable<void> {
          // Act
          auto err = co_await client.async_connect();

          // Assert
          REQUIRE(err);
          REQUIRE(err == ErrorCode::NotAuthorized);
          co_await client.async_close();
        },
        rethrow);
    // NOLINTEND
  }

  SECTION("when host is unavailable")
  {
    // Arrange
    auto unavailable_config = config();
    unavailable_config.host = "invalid_host";
    auto client = hacpp::mqtt::AsyncMqttClient2{strand, unavailable_config};
    // NOLINTBEGIN
    boost::asio::co_spawn(
        strand,
        [client = std::move(client)]() mutable -> boost::asio::awaitable<void> {
          // Act
          auto err = co_await client.async_connect();

          // Assert
          REQUIRE(err);
          REQUIRE(err == ErrorCode::HostNotFound);
          co_await client.async_close();
        },
        rethrow);
    // NOLINTEND
  }

  SECTION("when port is unavailable")
  {
    // Arrange
    auto unavailable_config = config();
    unavailable_config.port = "9999";
    auto client = hacpp::mqtt::AsyncMqttClient2{strand, unavailable_config};
    // NOLINTBEGIN
    boost::asio::co_spawn(
        strand,
        [client = std::move(client)]() mutable -> boost::asio::awaitable<void> {
          // Act
          auto err = co_await client.async_connect();

          // Assert
          REQUIRE(err);
          REQUIRE(err == ErrorCode::ConnectionRefused);
          co_await client.async_close();
        },
        rethrow);
    // NOLINTEND
  }
}

TEST_CASE("Client is not operational when disconnected", "[client]")
{
  // Arrange
  auto io = boost::asio::io_context{};
  auto strand = boost::asio::make_strand(io);
  SECTION("cannot publish")
  {
    boost::asio::co_spawn(strand, require_disconnected_publish(strand), rethrow);
  }

  SECTION("cannot subscribe")
  {
    boost::asio::co_spawn(strand, require_disconnected_subscribe(strand), rethrow);
  }

  io.run();
}

namespace {
// Helper to run proxy commands
void run_proxy(const std::string& cmd)
{
  std::string path = "/home/env/manage_proxy.py";

  if (!std::filesystem::exists(path)) {
    path = std::string(INTEGRATION_TEST_ENV_DIR) + "/manage_proxy.py";
  }

  spdlog::debug("Running proxy command: '{}' using script: {}", cmd, path);

  auto full_cmd = "python3 " + path + " " + cmd;
  // NOLINTNEXTLINE(concurrency-mt-unsafe, cert-env33-c): integration test invokes the proxy helper process.
  int res = std::system(full_cmd.c_str());

  if (res != 0) {
    spdlog::error("Failed to run proxy command: {} (exit code: {})", full_cmd, res);

    // Diagnostic: Check if python3 actually exists in a common location
    if (std::filesystem::exists("/usr/bin/python3")) {
      spdlog::info("/usr/bin/python3 exists. Attempting with absolute path...");
      full_cmd = "/usr/bin/python3 " + path + " " + cmd;
      // NOLINTNEXTLINE(concurrency-mt-unsafe, cert-env33-c): integration test invokes the proxy helper process.
      res = std::system(full_cmd.c_str());
      if (res == 0) {
        return;
      }
    } else {
      spdlog::error("/usr/bin/python3 DOES NOT EXIST in the container!");
    }
  }
}
} // namespace

TEST_CASE("Client can autoreconnect", "[client][autoreconnect]")
{
  // Arrange
  auto io = boost::asio::io_context{};
  auto strand = boost::asio::make_strand(io);

  auto proxy_config = config();
  proxy_config.port = "1884";
  auto client = std::make_shared<hacpp::mqtt::AsyncMqttClient2>(strand, proxy_config);

  run_proxy("setup");
  run_proxy("reconnect");

  bool reconnected_signaled = false;

  // Act && Assert
  // NOLINTBEGIN
  boost::asio::co_spawn(
      strand,
      [&, client]() -> boost::asio::awaitable<void> {
        auto err = co_await client->async_connect();
        REQUIRE(!err);

        while (true) {
          auto res = co_await client->async_recv();
          if (!res) {
            spdlog::info("Recv error in test: {}", res.error().message());
            if (res.error() == ErrorCode::SessionLost) {
              reconnected_signaled = true;
              break;
            }
            if (res.error() == ErrorCode::Disconnected) {
              break;
            }
          }
        }
        co_await client->async_close();
      },
      rethrow);

  boost::asio::co_spawn(
      strand,
      [&]() -> boost::asio::awaitable<void> {
        boost::asio::steady_timer timer{strand};

        timer.expires_after(std::chrono::milliseconds(500));
        co_await timer.async_wait(boost::asio::use_awaitable);

        spdlog::info("TEST: Disconnecting proxy...");
        run_proxy("disconnect");

        timer.expires_after(std::chrono::seconds(2));
        co_await timer.async_wait(boost::asio::use_awaitable);

        spdlog::info("TEST: Reconnecting proxy...");
        run_proxy("reconnect");
      },
      rethrow);

  // Global timeout for the test to prevent hanging
  boost::asio::co_spawn(
      strand,
      [&]() -> boost::asio::awaitable<void> {
        boost::asio::steady_timer timer{strand};
        timer.expires_after(std::chrono::seconds(15));
        co_await timer.async_wait(boost::asio::use_awaitable);
        io.stop();
      },
      rethrow);
  // NOLINTEND

  io.run();

  CHECK(reconnected_signaled);
}

TEST_CASE("Publish waits for autoreconnect before sending", "[client][autoreconnect_defer_publish]")
{
  // Arrange
  auto io = boost::asio::io_context{};
  auto strand = boost::asio::make_strand(io);

  auto proxy_config = config();
  proxy_config.port = "1884";
  auto client = std::make_shared<hacpp::mqtt::AsyncMqttClient2>(strand, proxy_config);

  run_proxy("setup");
  run_proxy("reconnect");

  auto reconnected_signaled = false;
  auto publish_completed = false;
  auto publish_err = Error{};

  // NOLINTBEGIN
  boost::asio::co_spawn(
      strand,
      [&, client]() -> boost::asio::awaitable<void> {
        auto err = co_await client->async_connect();
        REQUIRE(!err);

        while (true) {
          auto res = co_await client->async_recv();
          if (!res) {
            spdlog::info("Recv error in test: {}", res.error().message());
            if (res.error() == ErrorCode::SessionLost) {
              reconnected_signaled = true;
              break;
            }
            if (res.error() == ErrorCode::Disconnected) {
              break;
            }
          }
        }
      },
      rethrow);

  boost::asio::co_spawn(
      strand,
      [&, client]() -> boost::asio::awaitable<void> {
        auto timer = boost::asio::steady_timer{strand};

        timer.expires_after(std::chrono::milliseconds(500));
        co_await timer.async_wait(boost::asio::use_awaitable);

        spdlog::info("TEST: Disconnecting proxy...");
        run_proxy("disconnect");

        boost::asio::co_spawn(
            strand,
            [&, client]() -> boost::asio::awaitable<void> {
              auto timer = boost::asio::steady_timer{strand};
              timer.expires_after(std::chrono::seconds(2));
              co_await timer.async_wait(boost::asio::use_awaitable);

              publish_err = co_await client->async_publish(
                  "test/topic",
                  "payload during reconnect",
                  async_mqtt::qos::at_least_once);
              publish_completed = true;
            },
            rethrow);

        timer.expires_after(std::chrono::seconds(5));
        co_await timer.async_wait(boost::asio::use_awaitable);

        spdlog::info("TEST: Reconnecting proxy...");
        run_proxy("reconnect");
      },
      rethrow);

  boost::asio::co_spawn(
      strand,
      [&]() -> boost::asio::awaitable<void> {
        boost::asio::steady_timer timer{strand};
        timer.expires_after(std::chrono::seconds(15));
        co_await timer.async_wait(boost::asio::use_awaitable);
        io.stop();
      },
      rethrow);
  // NOLINTEND

  io.run();

  CHECK(reconnected_signaled);
  CHECK(publish_completed);
  CHECK(!publish_err);
}

TEST_CASE("Subscribe waits for autoreconnect before sending", "[client][autoreconnect_defere_subscribe]")
{
  // Arrange
  auto io = boost::asio::io_context{};
  auto strand = boost::asio::make_strand(io);

  auto proxy_config = config();
  proxy_config.port = "1884";
  auto client = std::make_shared<hacpp::mqtt::AsyncMqttClient2>(strand, proxy_config);

  run_proxy("setup");
  run_proxy("reconnect");

  auto reconnected_signaled = false;
  auto subscribe_completed = false;
  auto subscribe_err = Error{};

  // NOLINTBEGIN
  boost::asio::co_spawn(
      strand,
      [&, client]() -> boost::asio::awaitable<void> {
        auto err = co_await client->async_connect();
        REQUIRE(!err);

        while (true) {
          auto res = co_await client->async_recv();
          if (!res) {
            spdlog::info("Recv error in test: {}", res.error().message());
            if (res.error() == ErrorCode::SessionLost) {
              reconnected_signaled = true;
              break;
            }
            if (res.error() == ErrorCode::Disconnected) {
              break;
            }
          }
        }
      },
      rethrow);

  boost::asio::co_spawn(
      strand,
      [&, client]() -> boost::asio::awaitable<void> {
        auto timer = boost::asio::steady_timer{strand};

        timer.expires_after(std::chrono::milliseconds(500));
        co_await timer.async_wait(boost::asio::use_awaitable);

        spdlog::info("TEST: Disconnecting proxy...");
        run_proxy("disconnect");

        boost::asio::co_spawn(
            strand,
            [&, client]() -> boost::asio::awaitable<void> {
              auto timer = boost::asio::steady_timer{strand};
              timer.expires_after(std::chrono::seconds(2));
              co_await timer.async_wait(boost::asio::use_awaitable);

              auto topics = std::vector<hacpp::mqtt::TopicSubopts>{
                  {"test/topic", async_mqtt::qos::at_least_once}
              };
              subscribe_err = co_await client->async_subscribe(topics);
              subscribe_completed = true;
            },
            rethrow);

        timer.expires_after(std::chrono::seconds(5));
        co_await timer.async_wait(boost::asio::use_awaitable);

        spdlog::info("TEST: Reconnecting proxy...");
        run_proxy("reconnect");
      },
      rethrow);

  boost::asio::co_spawn(
      strand,
      [&]() -> boost::asio::awaitable<void> {
        boost::asio::steady_timer timer{strand};
        timer.expires_after(std::chrono::seconds(15));
        co_await timer.async_wait(boost::asio::use_awaitable);
        io.stop();
      },
      rethrow);
  // NOLINTEND

  io.run();

  CHECK(reconnected_signaled);
  CHECK(subscribe_completed);
  CHECK(!subscribe_err);
}
