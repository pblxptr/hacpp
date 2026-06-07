#include "config.h"

#include <hacpp/async_mqtt_client.h>
#include <hacpp/entity.h>
#include <hacpp/hacpp.h>
#include <hacpp/binary_sensor.h>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/impl/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/strand.hpp>
#include <boost/json/parse.hpp>
#include <catch2/catch_test_macros.hpp>

#include <memory>
#include <utility>
#include <vector>

namespace {

using hacpp::mqtt::Availability;
using hacpp::mqtt::BinarySensor;
using hacpp::mqtt::ClientType;
using hacpp::mqtt::Factory;
using hacpp::mqtt::PublishPacket;
using hacpp::mqtt::QoS;
using hacpp::mqtt::TopicSubopts;
using hacpp::mqtt::default_component_availability_topic;
using hacpp::mqtt::default_component_discovery_topic;
using hacpp::mqtt::default_component_state_topic;

constexpr auto UniqueId = "binary_sensor_unique_id";

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
      {default_component_discovery_topic(BinarySensor::Defs::Component,    UniqueId), QoS::at_most_once},
      {default_component_state_topic(BinarySensor::Defs::Component,        UniqueId), QoS::at_most_once},
      {default_component_availability_topic(BinarySensor::Defs::Component, UniqueId), QoS::at_most_once}
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

TEST_CASE("Binary sensor provides all required options during discovery", "[binary_sensor]")
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
        auto binary_sensor =
            Factory<BinarySensor>(UniqueId, std::move(entity_client))
            .create();
        // clang-format on

        // Act
        auto err = co_await binary_sensor.async_discovery();
        auto packet = co_await async_recv_packet<PublishPacket>(verifier_client);

        // Assert
        REQUIRE(!err);
        REQUIRE(packet.topic() == default_component_discovery_topic(BinarySensor::Defs::Component, UniqueId));
        auto pobj = boost::json::parse(packet.payload());
        REQUIRE(pobj.as_object().contains(BinarySensor::Opt::StateTopic.key));
        REQUIRE(!pobj.as_object()[BinarySensor::Opt::StateTopic.key].as_string().empty());

        co_await binary_sensor.async_close();
        co_await verifier_client->async_close();
      },
      // NOLINTEND(cppcoreguidelines-avoid-capturing-lambda-coroutines)
      rethrow);

  io.run();
}

TEST_CASE("Binary sensor can update its state", "[binary_sensor]")
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
        auto binary_sensor = Factory<BinarySensor>(UniqueId, std::move(entity_client))
                .set(Availability::Opt::Topic,
                     default_component_availability_topic(
                         BinarySensor::Defs::Component, UniqueId))
                .create();
        // clang-format on
        auto err1 = co_await binary_sensor.async_discovery();
        auto packet = co_await async_recv_packet<PublishPacket>(verifier_client);
        REQUIRE(!err1);

        SECTION("provides 'on' state")
        {
          // Act
          auto err = co_await binary_sensor.async_update_state(true);
          auto packet = co_await async_recv_packet<PublishPacket>(verifier_client);

          // Assert
          REQUIRE(!err);
          REQUIRE(packet.topic() == binary_sensor.config().at(BinarySensor::Opt::StateTopic));
          REQUIRE(packet.payload() == BinarySensor::Defs::PayloadOn);
          co_await binary_sensor.async_close();
          co_await verifier_client->async_close();
        }

        SECTION("provides 'off' state")
        {
          // Act
          auto err = co_await binary_sensor.async_update_state(false);
          auto packet = co_await async_recv_packet<PublishPacket>(verifier_client);

          // Assert
          REQUIRE(!err);
          REQUIRE(packet.topic() == binary_sensor.config().at(BinarySensor::Opt::StateTopic));
          REQUIRE(packet.payload() == BinarySensor::Defs::PayloadOff);
          co_await binary_sensor.async_close();
          co_await verifier_client->async_close();
        }
      },
      // NOLINTEND(cppcoreguidelines-avoid-capturing-lambda-coroutines)
      rethrow);

  io.run();
}

TEST_CASE("Binary sensor can update its availability", "[binary_sensor]")
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
        auto binary_sensor = Factory<BinarySensor>(UniqueId, std::move(entity_client))
                .set(Availability::Opt::Topic,
                     default_component_availability_topic(
                         BinarySensor::Defs::Component, UniqueId))
                .create();
        // clang-format on
        auto err_disc = co_await binary_sensor.async_discovery();
        REQUIRE(!err_disc);
        auto packet_disc = co_await async_recv_packet<PublishPacket>(verifier_client);

        SECTION("provides 'online' state")
        {
          // Act
          auto err = co_await binary_sensor.async_update_availability(true);
          auto packet = co_await async_recv_packet<PublishPacket>(verifier_client);

          // Assert
          REQUIRE(!err);
          REQUIRE(packet.topic() == default_component_availability_topic(BinarySensor::Defs::Component, UniqueId));
          REQUIRE(packet.payload() == Availability::Defs::PayloadAvailable);
        }

        SECTION("provides 'offline' state")
        {
          // Act
          auto err = co_await binary_sensor.async_update_availability(false);
          auto packet = co_await async_recv_packet<PublishPacket>(verifier_client);

          // Assert
          REQUIRE(!err);
          REQUIRE(packet.topic() == default_component_availability_topic(BinarySensor::Defs::Component, UniqueId));
          REQUIRE(packet.payload() == Availability::Defs::PayloadNotAvailable);
        }

        co_await binary_sensor.async_close();
        co_await verifier_client->async_close();
      },
      // NOLINTEND(cppcoreguidelines-avoid-capturing-lambda-coroutines)
      rethrow);

  io.run();
}
