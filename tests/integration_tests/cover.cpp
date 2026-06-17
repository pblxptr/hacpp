#include "config.h"

#include <hacpp/async_mqtt_client.h>
#include <hacpp/cover.h>
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
#include <string>
#include <utility>
#include <vector>

namespace {

using hacpp::mqtt::ClientType;
using hacpp::mqtt::Cover;
using hacpp::mqtt::CoverCfg;
using hacpp::mqtt::default_component_availability_topic;
using hacpp::mqtt::default_component_command_topic;
using hacpp::mqtt::default_component_discovery_topic;
using hacpp::mqtt::default_component_state_topic;
using hacpp::mqtt::Factory;
using hacpp::mqtt::PublishPacket;
using hacpp::mqtt::QoS;
using hacpp::mqtt::TopicSubopts;

constexpr auto UniqueId = "cover_unique_id";

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
      {default_component_discovery_topic(CoverCfg::Defs::Component,    UniqueId), QoS::at_most_once},
      {default_component_command_topic(CoverCfg::Defs::Component,      UniqueId), QoS::at_most_once},
      {default_component_state_topic(CoverCfg::Defs::Component,        UniqueId), QoS::at_most_once},
      {default_component_availability_topic(CoverCfg::Defs::Component, UniqueId), QoS::at_most_once}
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

TEST_CASE("Cover provides all required options during discovery", "[cover]")
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
        auto cover = Factory<Cover>(UniqueId, std::move(entity_client))
          .create();
        // clang-format on
        // Act
        auto err = co_await cover.async_discovery();
        auto packet = co_await async_recv_packet<PublishPacket>(verifier_client);

        // Assert
        REQUIRE(!err);
        REQUIRE(packet.topic() == default_component_discovery_topic(CoverCfg::Defs::Component, UniqueId));
        auto pobj = boost::json::parse(packet.payload());
        REQUIRE(pobj.as_object().contains(CoverCfg::Opt::CommandTopic.key));
        REQUIRE(!pobj.as_object()[CoverCfg::Opt::CommandTopic.key].as_string().empty());

        co_await cover.async_close();
        co_await verifier_client->async_close();
      },
      // NOLINTEND(cppcoreguidelines-avoid-capturing-lambda-coroutines)
      rethrow);

  io.run();
}

TEST_CASE("Cover can receive commands", "[cover]")
{
  // Arrange
  auto io = boost::asio::io_context{};
  auto strand = boost::asio::make_strand(io);
  static constexpr auto default_delay = std::chrono::milliseconds{100};

  boost::asio::co_spawn(
      strand,
      // NOLINTBEGIN(cppcoreguidelines-avoid-capturing-lambda-coroutines)
      // clang-tidy 19 does not recognize C++23 explicit object parameters as the safe pattern here.
      [&, strand](this auto /* self */) -> boost::asio::awaitable<void> {
        auto entity_client = co_await get_client(strand);
        auto verifier_client = co_await get_verifier(strand);

        auto received_command = std::make_shared<std::string>();

        // clang-format off
        auto cover = Factory<Cover>(UniqueId, std::move(entity_client))
          .on_open([received_command](this auto /* self */) -> boost::asio::awaitable<void> {
                           *received_command = "OPEN";
                           co_return;
                         })
          .on_close([received_command](this auto /* self */) -> boost::asio::awaitable<void> {
                           *received_command = "CLOSE";
                           co_return;
                         })
          .on_stop([received_command](this auto /* self */) -> boost::asio::awaitable<void> {
                           *received_command = "STOP";
                           co_return;
                         })
          .create();
        // clang-format on

        auto err_disc = co_await cover.async_setup();
        REQUIRE(!err_disc);
        auto packet_disc = co_await async_recv_packet<PublishPacket>(verifier_client);

        // Start cover loop in background
        boost::asio::co_spawn(strand, [&]() -> boost::asio::awaitable<void> { co_await cover.async_run(); }, rethrow);

        SECTION("OPEN command")
        {
          auto err_pub = co_await verifier_client->async_publish(
              default_component_command_topic(CoverCfg::Defs::Component, UniqueId),
              CoverCfg::Defs::PayloadOpen);
          REQUIRE(!err_pub);

          auto timer = boost::asio::steady_timer{strand};
          timer.expires_after(default_delay);
          co_await timer.async_wait(boost::asio::use_awaitable);
          REQUIRE(*received_command == "OPEN");
        }

        SECTION("CLOSE command")
        {
          auto err_pub = co_await verifier_client->async_publish(
              default_component_command_topic(CoverCfg::Defs::Component, UniqueId),
              CoverCfg::Defs::PayloadClose);
          REQUIRE(!err_pub);

          auto timer = boost::asio::steady_timer{strand};
          timer.expires_after(default_delay);
          co_await timer.async_wait(boost::asio::use_awaitable);
          REQUIRE(*received_command == "CLOSE");
        }

        SECTION("STOP command")
        {
          auto err_pub = co_await verifier_client->async_publish(
              default_component_command_topic(CoverCfg::Defs::Component, UniqueId),
              CoverCfg::Defs::PayloadStop);
          REQUIRE(!err_pub);

          auto timer = boost::asio::steady_timer{strand};
          timer.expires_after(default_delay);
          co_await timer.async_wait(boost::asio::use_awaitable);
          REQUIRE(*received_command == "STOP");
        }

        co_await cover.async_close();
        co_await verifier_client->async_close();
      },
      // NOLINTEND(cppcoreguidelines-avoid-capturing-lambda-coroutines)
      rethrow);

  io.run();
}

TEST_CASE("Cover state update", "[cover]")
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
        auto cover = Factory<Cover>(UniqueId, std::move(entity_client))
                         .set(CoverCfg::Opt::StateTopic,
                              default_component_state_topic(
                                  CoverCfg::Defs::Component, UniqueId))
                         .create();
        // clang-format on
        auto err_disc = co_await cover.async_discovery();
        REQUIRE(!err_disc);
        auto packet_disc = co_await async_recv_packet<PublishPacket>(verifier_client);

        // Act
        auto err = co_await cover.async_update_state(CoverCfg::Defs::StateOpen);
        auto packet = co_await async_recv_packet<PublishPacket>(verifier_client);

        // Assert
        REQUIRE(!err);
        REQUIRE(packet.topic() == default_component_state_topic(CoverCfg::Defs::Component, UniqueId));
        REQUIRE(packet.payload() == CoverCfg::Defs::StateOpen);

        co_await cover.async_close();
        co_await verifier_client->async_close();
      },
      // NOLINTEND(cppcoreguidelines-avoid-capturing-lambda-coroutines)
      rethrow);

  io.run();
}
