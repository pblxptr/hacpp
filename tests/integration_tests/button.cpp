#include "config.h"

#include <hacpp/async_mqtt_client.h>
#include <hacpp/button.h>
#include <hacpp/entity.h>
#include <hacpp/hacpp.h>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/impl/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/json/parse.hpp>
#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <memory>
#include <utility>
#include <vector>

namespace {

using hacpp::mqtt::Availability;
using hacpp::mqtt::Button;
using hacpp::mqtt::ButtonCfg;
using hacpp::mqtt::ClientType;
using hacpp::mqtt::default_component_availability_topic;
using hacpp::mqtt::default_component_command_topic;
using hacpp::mqtt::default_component_discovery_topic;
using hacpp::mqtt::Factory;
using hacpp::mqtt::PublishPacket;
using hacpp::mqtt::QoS;
using hacpp::mqtt::TopicSubopts;

constexpr auto UniqueId = "button_unique_id";

boost::asio::awaitable<ClientType> get_client(boost::asio::any_io_executor exe)
{
  auto client = ClientType{exe, config()};
  auto err = co_await client.async_connect();
  REQUIRE(!err);

  co_return client;
}

boost::asio::awaitable<std::shared_ptr<ClientType>> get_verifier(boost::asio::any_io_executor exe)
{
  auto client = std::make_shared<ClientType>(exe, config());
  auto err = co_await client->async_connect();
  REQUIRE(!err);

  auto sub_topics = std::vector<TopicSubopts>{
      {default_component_discovery_topic(ButtonCfg::Defs::Component,    UniqueId), QoS::at_most_once},
      {default_component_command_topic(ButtonCfg::Defs::Component,      UniqueId), QoS::at_most_once},
      {default_component_availability_topic(ButtonCfg::Defs::Component, UniqueId), QoS::at_most_once}
  };

  err = co_await client->async_subscribe(sub_topics);
  REQUIRE(!err);

  co_return client;
}

template <typename T>
boost::asio::awaitable<T> async_recv_packet(std::shared_ptr<ClientType> client)
{
  auto res = co_await client->async_recv();
  REQUIRE(res.has_value());

  auto* packet = res->template get_if<T>();
  REQUIRE(packet);

  co_return *packet;
}

} // namespace

TEST_CASE("Button provides all required options during discovery", "[button]")
{
  // Arrange
  auto io = boost::asio::io_context{};
  auto strand = boost::asio::make_strand(io);
  boost::asio::co_spawn(
      strand,
      // NOLINTBEGIN(cppcoreguidelines-avoid-capturing-lambda-coroutines)
      // clang-tidy 19 does not recognize C++23 explicit object parameters as the safe pattern here.
      [strand](this auto /* self */) -> boost::asio::awaitable<void> {
        auto entity_client = co_await get_client(strand);
        auto verifier_client = co_await get_verifier(strand);
        // clang-format off
        auto button = Factory<Button>(UniqueId, std::move(entity_client))
          .create();
        // clang-format on
        // Act
        auto err = co_await button.async_discovery();
        auto packet = co_await async_recv_packet<PublishPacket>(verifier_client);

        // Assert
        REQUIRE(!err);
        REQUIRE(packet.topic() == default_component_discovery_topic(ButtonCfg::Defs::Component, UniqueId));
        auto pobj = boost::json::parse(packet.payload());
        REQUIRE(pobj.as_object().contains(ButtonCfg::Opt::CommandTopic.key));
        REQUIRE(!pobj.as_object()[ButtonCfg::Opt::CommandTopic.key].as_string().empty());

        co_await button.async_close();
        co_await verifier_client->async_close();
      },
      // NOLINTEND(cppcoreguidelines-avoid-capturing-lambda-coroutines)
      rethrow);

  io.run();
}

TEST_CASE("Button can receive press command", "[button]")
{
  // Arrange
  auto io = boost::asio::io_context{};
  auto strand = boost::asio::make_strand(io);
  auto button = std::shared_ptr<Button<>>{};
  static constexpr auto default_delay = std::chrono::milliseconds{100};

  boost::asio::co_spawn(
      strand,
      // NOLINTBEGIN(cppcoreguidelines-avoid-capturing-lambda-coroutines)
      // clang-tidy 19 does not recognize C++23 explicit object parameters as the safe pattern here.
      [strand, button](this auto /* self */) -> boost::asio::awaitable<void> {
        auto entity_client = co_await get_client(strand);
        auto verifier_client = co_await get_verifier(strand);
        bool pressed = false;
        // clang-format off
        button = std::make_shared<Button<>>(Factory<Button>(UniqueId, std::move(entity_client))
                .on_press([&pressed](this auto /* self */) -> boost::asio::awaitable<void> {
                  pressed = true;
                  co_return;
                })
                .create());
        // clang-format on
        auto err_disc = co_await button->async_setup();
        REQUIRE(!err_disc);
        auto packet_disc = co_await async_recv_packet<PublishPacket>(verifier_client);

        // Start button loop in background
        boost::asio::co_spawn(strand, [&]() -> boost::asio::awaitable<void> { co_await button->async_run(); }, rethrow);

        // Act
        auto err_pub = co_await verifier_client->async_publish(
            default_component_command_topic(ButtonCfg::Defs::Component, UniqueId),
            ButtonCfg::Defs::PayloadPress);
        REQUIRE(!err_pub);

        // Give some time for packet delivery and processing
        auto timer = boost::asio::steady_timer{strand};
        timer.expires_after(default_delay);
        co_await timer.async_wait(boost::asio::use_awaitable);

        // Assert
        REQUIRE(pressed);

        co_await button->async_close();
        co_await verifier_client->async_close();
      },
      // NOLINTEND(cppcoreguidelines-avoid-capturing-lambda-coroutines)
      rethrow);

  io.run();
}

TEST_CASE("Button availability", "[button]")
{
  // Arrange
  auto io = boost::asio::io_context{};
  auto strand = boost::asio::make_strand(io);

  boost::asio::co_spawn(
      strand,
      // NOLINTBEGIN(cppcoreguidelines-avoid-capturing-lambda-coroutines)
      // clang-tidy 19 does not recognize C++23 explicit object parameters as the safe pattern here.
      [&, strand](this auto /* self */) -> boost::asio::awaitable<void> {
        auto entity_client = co_await get_client(strand);
        auto verifier_client = co_await get_verifier(strand);
        // clang-format off
        auto button = Factory<Button>(UniqueId, std::move(entity_client))
                          .set(Availability::Opt::Topic,
                               default_component_availability_topic(
                                   ButtonCfg::Defs::Component, UniqueId))
                          .create();
        // clang-format on
        auto err_disc = co_await button.async_discovery();
        REQUIRE(!err_disc);
        auto packet_disc = co_await async_recv_packet<PublishPacket>(verifier_client);

        SECTION("provides 'online' state")
        {
          // Act
          auto err = co_await button.async_update_availability(true);
          auto packet = co_await async_recv_packet<PublishPacket>(verifier_client);

          // Assert
          REQUIRE(!err);
          REQUIRE(packet.topic() == default_component_availability_topic(ButtonCfg::Defs::Component, UniqueId));
          REQUIRE(packet.payload() == Availability::Defs::PayloadAvailable);
        }

        SECTION("provides 'offline' state")
        {
          // Act
          auto err = co_await button.async_update_availability(false);
          auto packet = co_await async_recv_packet<PublishPacket>(verifier_client);

          // Assert
          REQUIRE(!err);
          REQUIRE(packet.topic() == default_component_availability_topic(ButtonCfg::Defs::Component, UniqueId));
          REQUIRE(packet.payload() == Availability::Defs::PayloadNotAvailable);
        }

        co_await button.async_close();
        co_await verifier_client->async_close();
      },
      // NOLINTEND(cppcoreguidelines-avoid-capturing-lambda-coroutines)
      rethrow);

  io.run();
}
