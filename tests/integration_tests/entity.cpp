#include "config.h"

#include <hacpp/async_mqtt_client.h>
#include <hacpp/entity.h>
#include <hacpp/error.h>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <catch2/catch_test_macros.hpp>
#include <spdlog/spdlog.h>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <string>
#include <utility>

namespace {

using hacpp::mqtt::ClientType;
using hacpp::mqtt::Entity;
using hacpp::mqtt::EntityCfg;
using hacpp::mqtt::Error;
using hacpp::mqtt::ErrorCode;
using hacpp::mqtt::QoS;

struct SetupCounters
{
    int discovery_calls{0};
    int subscribe_calls{0};
};

class TestEntity : protected Entity<TestEntity>
{
    using Base = Entity<TestEntity>;
    friend Base;

  public:
    using Base::async_close;
    using Base::async_recv;
    using Base::async_setup;

    struct Config
    {
        QoS qos;
        EntityCfg cfg;
    };

    TestEntity(ClientType client, std::shared_ptr<SetupCounters> counters)
        : Base{std::move(client)}
        , counters_{std::move(counters)}
    {}

  protected:
    boost::asio::awaitable<Error> async_discovery_impl()
    {
      ++counters_->discovery_calls;
      co_return ErrorCode::Success;
    }

    boost::asio::awaitable<Error> async_subscribe_impl()
    {
      ++counters_->subscribe_calls;
      co_return ErrorCode::Success;
    }

  private:
    Config config_{.qos = QoS::at_most_once, .cfg = {}};
    std::shared_ptr<SetupCounters> counters_;
};

void run_proxy(const std::string& cmd)
{
  auto path = std::string{"/home/env/manage_proxy.py"};

  if (!std::filesystem::exists(path)) {
    path = std::string{INTEGRATION_TEST_ENV_DIR} + "/manage_proxy.py";
  }

  auto full_cmd = "python3 " + path + " " + cmd;
  // NOLINTNEXTLINE(concurrency-mt-unsafe, cert-env33-c): integration test invokes the proxy helper process.
  auto res = std::system(full_cmd.c_str());
  if (res != 0) {
    spdlog::error("Failed to run proxy command: {} (exit code: {})", full_cmd, res);
  }
}

boost::asio::awaitable<void> wait_for_setup_replay(
    std::shared_ptr<SetupCounters> counters,
    int initial_discovery_calls,
    int initial_subscribe_calls)
{
  auto timer = boost::asio::steady_timer{co_await boost::asio::this_coro::executor};
  for (auto attempt = 0; attempt < 30; ++attempt) {
    if (counters->discovery_calls == initial_discovery_calls + 1 &&
        counters->subscribe_calls == initial_subscribe_calls + 1) {
      co_return;
    }

    timer.expires_after(std::chrono::milliseconds{100});
    co_await timer.async_wait(boost::asio::use_awaitable);
  }
}

} // namespace

TEST_CASE("Entity calls setup again after client reconnect", "[entity][autoreconnect]")
{
  // Arrange
  auto io = boost::asio::io_context{};
  auto strand = boost::asio::make_strand(io);
  auto proxy_config = config();
  proxy_config.port = "1884";
  auto counters = std::make_shared<SetupCounters>();

  run_proxy("setup");
  run_proxy("reconnect");

  // Act && Assert
  // NOLINTBEGIN(cppcoreguidelines-avoid-capturing-lambda-coroutines)
  // clang-tidy 19 does not recognize C++23 explicit object parameters as the safe pattern here.
  boost::asio::co_spawn(
      strand,
      [strand, proxy_config, counters]() mutable -> boost::asio::awaitable<void> {
        auto client = ClientType{strand, proxy_config};
        auto err = co_await client.async_connect();
        REQUIRE(!err);

        auto entity = std::make_shared<TestEntity>(std::move(client), counters);

        err = co_await entity->async_setup();
        REQUIRE(!err);
        const auto initial_discovery_calls = counters->discovery_calls;
        const auto initial_subscribe_calls = counters->subscribe_calls;
        REQUIRE(initial_discovery_calls == 1);
        REQUIRE(initial_subscribe_calls == 1);

        boost::asio::co_spawn(
            strand,
            [entity]() -> boost::asio::awaitable<void> {
              auto packet = co_await entity->async_recv();
              if (!packet && packet.error() != ErrorCode::Disconnected) {
                REQUIRE(!packet.error());
              }
            },
            rethrow);

        auto timer = boost::asio::steady_timer{strand};
        timer.expires_after(std::chrono::milliseconds{500});
        co_await timer.async_wait(boost::asio::use_awaitable);

        run_proxy("disconnect");

        timer.expires_after(std::chrono::seconds{2});
        co_await timer.async_wait(boost::asio::use_awaitable);

        run_proxy("reconnect");

        co_await wait_for_setup_replay(counters, initial_discovery_calls, initial_subscribe_calls);
        co_await entity->async_close();
      },
      rethrow);
  // NOLINTEND(cppcoreguidelines-avoid-capturing-lambda-coroutines)

  io.run();

  // Assert
  // Initial increment + one after reconnect
  REQUIRE(counters->discovery_calls == 2);
  REQUIRE(counters->subscribe_calls == 2);
}
