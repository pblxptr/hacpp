#include "config.h"

#include <hacpp/shared_mqtt_client.h>

#include <catch2/catch_test_macros.hpp>
#include <fmt/format.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <memory>
#include <numeric>
#include <random>
#include <string>
#include <thread>
#include <vector>

using hacpp::mqtt::ClientType;
using hacpp::mqtt::PublishPacket;
using hacpp::mqtt::QoS;
using hacpp::mqtt::TopicSubopts;

static constexpr auto NumberOfProxies = 50;
static constexpr auto NumberOfPublishPerProxy = 100;
static constexpr auto TotalMsgExchange = NumberOfProxies * NumberOfPublishPerProxy;
static constexpr auto ExchangeTimeout = std::chrono::seconds{180};

static std::atomic<bool> PublishDone = false;

static auto get_payload_press(int id)
{
  return fmt::format("press-{}", id);
}

static auto get_command_topic(int id)
{
  return fmt::format("hacpp/shared-client-test/{}/command", id);
}

static auto spawn_publisher_thread(std::shared_ptr<std::atomic<bool>> ready_to_publish)
{
  auto io = std::make_shared<boost::asio::io_context>();
  auto strand = boost::asio::make_strand(*io);

  // Fill the list of ids e.g 3 clients each 4 msgs [ 0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2 ]
  auto publist = std::vector<int>{};
  for (auto i = 0; i < NumberOfProxies; i++) {
    for (auto j = 0; j < NumberOfPublishPerProxy; j++) {
      publist.push_back(i);
    }
  }

  // Shuffle the list
  std::shuffle(publist.begin(), publist.end(), std::mt19937{std::random_device{}()});

  boost::asio::co_spawn(
      strand,
      [io, strand, publist = std::move(publist)](this auto /* self */) -> boost::asio::awaitable<void> {
        auto client = std::make_shared<ClientType>(strand, config());
        auto err = co_await client->async_connect();
        REQUIRE(!err);

        for (const auto& id : publist) {
          auto err = co_await client->async_publish(get_command_topic(id), get_payload_press(id));
          REQUIRE(!err);
        }
        io->stop();
      },
      boost::asio::detached);

  return std::jthread([io, ready_to_publish = std::move(ready_to_publish)]() {
    while (!ready_to_publish->load()) {
      std::this_thread::sleep_for(std::chrono::milliseconds{10});
    }

    io->run();
    PublishDone = true;
    spdlog::debug("Publish done");
  });
}

TEST_CASE("SharedAsyncMqttClient can handle multiple proxies", "[integration][shared_async_mqtt_client][robustness]")
{
  // Arrange
  PublishDone = false;
  auto counters = std::vector<int>(NumberOfProxies, 0);
  auto subscribed_count = std::make_shared<std::atomic<int>>(0);
  auto ready_to_publish = std::make_shared<std::atomic<bool>>(false);

  auto io = boost::asio::io_context{};
  auto strand = boost::asio::make_strand(io);

  // Act
  boost::asio::co_spawn(
      strand,
      [strand, &counters, subscribed_count, ready_to_publish](this auto /* self */) -> boost::asio::awaitable<void> {
        auto shared_client =
            hacpp::mqtt::SharedAsyncMqttClient::create(hacpp::mqtt::AsyncMqttClient2{strand, config()});

        auto err = co_await shared_client->async_connect();
        REQUIRE(!err);

        boost::asio::co_spawn(
            shared_client->executor(),
            [shared_client](this auto /* self */) -> boost::asio::awaitable<void> {
              while (true) {
                co_await shared_client->async_recv();
              }
            },
            boost::asio::detached);

        for (int i = 0; i < NumberOfProxies; ++i) {
          boost::asio::co_spawn(
              shared_client->executor(),
              [shared_client, i, &counters, subscribed_count](this auto /* self */) -> boost::asio::awaitable<void> {
                auto proxy = shared_client->proxy();
                auto sub_topics = std::vector<TopicSubopts>{
                    {get_command_topic(i), QoS::at_least_once}
                };
                auto err = co_await proxy.async_subscribe(std::move(sub_topics));
                REQUIRE(!err);
                ++(*subscribed_count);

                while (true) {
                  auto result = co_await proxy.async_recv();
                  if (!result) {
                    co_return;
                  }

                  const auto& packet = result->template get<PublishPacket>();
                  if (packet.topic() == get_command_topic(i) && packet.payload() == get_payload_press(i)) {
                    ++counters[i];
                  }
                }
              },
              boost::asio::detached);
        }

        auto timer = boost::asio::steady_timer{strand};
        while (subscribed_count->load() != NumberOfProxies) {
          timer.expires_after(std::chrono::milliseconds{10});
          co_await timer.async_wait(boost::asio::use_awaitable);
        }
        ready_to_publish->store(true);
      },
      boost::asio::detached);

  // Infinity test duration protection guard
  boost::asio::co_spawn(
      strand,
      [strand, &io, &counters](this auto /* self */) -> boost::asio::awaitable<void> {
        auto timer = boost::asio::steady_timer{strand};
        auto end = boost::asio::steady_timer::clock_type::now() + ExchangeTimeout;

        auto total_received = [&counters]() { return std::accumulate(counters.begin(), counters.end(), 0); };

        while (true) {
          if (total_received() == TotalMsgExchange) {
            break;
          }

          if (boost::asio::steady_timer::clock_type::now() < end) {
            spdlog::debug("Timeout still valid... waiting, received: {}", total_received());
            timer.expires_after(std::chrono::milliseconds{100});
            co_await timer.async_wait(boost::asio::use_awaitable);
          } else {
            spdlog::debug("Timer expired...stopping, received: {}", total_received());
            break;
          }
        }

        io.stop();
      },
      boost::asio::detached);

  auto publisher_th = spawn_publisher_thread(ready_to_publish);

  io.run();

  if (publisher_th.joinable()) {
    publisher_th.join();
  }

  // Assert
  REQUIRE(PublishDone);
  const auto total_msgs = std::accumulate(counters.begin(), counters.end(), 0);

  REQUIRE(total_msgs == TotalMsgExchange);
  for (const auto& count : counters) {
    REQUIRE(count == NumberOfPublishPerProxy);
  }
}
