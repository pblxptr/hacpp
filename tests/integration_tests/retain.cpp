#include "config.h"

#include <hacpp/binary_sensor.h>
#include <hacpp/button.h>
#include <hacpp/cover.h>
#include <hacpp/sensor.h>
#include <hacpp/shared_mqtt_client.h>

#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <exception>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace {
using namespace hacpp::mqtt;

boost::asio::awaitable<void> require_success(boost::asio::awaitable<Error> operation)
{
  auto err = co_await std::move(operation);
  REQUIRE(!err);
}

// Subscribe only after publication: the broker must replay the retained message.
boost::asio::awaitable<void> require_retained(AsyncMqttClient& cleanup, std::string topic, std::string payload)
{
  auto verifier = AsyncMqttClient{cleanup.executor(), config()};
  co_await require_success(verifier.async_connect());
  auto topics = std::vector<TopicSubopts>{
      {topic, QoS::at_least_once}
  };
  co_await require_success(verifier.async_subscribe(std::move(topics)));
  auto result = co_await verifier.async_recv();
  REQUIRE(result.has_value());
  auto* packet = result->get_if<PublishPacket>();
  REQUIRE(packet);
  REQUIRE(packet->topic() == topic);
  REQUIRE(packet->payload() == payload);
  REQUIRE(packet->opts().get_retain() == async_mqtt::pub::retain::yes);
  co_await require_success(verifier.async_close());
  co_await require_success(cleanup.async_publish(topic, "", QoS::at_least_once, true));
}

template <template <typename> typename EntityType, typename Cfg, typename Client>
boost::asio::awaitable<void> check_entity(Client client, AsyncMqttClient& cleanup, const std::string& id)
{
  auto entity = factory<EntityType>(id, std::move(client)).create();
  co_await require_success(entity.async_discovery());
  co_await require_retained(
      cleanup,
      default_component_discovery_topic(Cfg::Defs::Component, id),
      entity.config().json());

  if constexpr (!std::is_same_v<Cfg, ButtonCfg>) {
    if constexpr (std::is_same_v<Cfg, BinarySensorCfg>) {
      co_await require_success(entity.async_update_state(false));
      co_await require_success(entity.async_update_state(true));
      co_await require_retained(cleanup, entity.config().at(Cfg::Opt::StateTopic), "ON");
    } else {
      co_await require_success(entity.async_update_state("old"));
      co_await require_success(entity.async_update_state("latest"));
      co_await require_retained(cleanup, entity.config().at(Cfg::Opt::StateTopic), "latest");
    }
  }
  co_await require_success(entity.async_close());
}

boost::asio::awaitable<void> check_all(boost::asio::any_io_executor executor, bool shared)
{
  auto cleanup = AsyncMqttClient{executor, config()};
  co_await require_success(cleanup.async_connect());
  auto suffix = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
  auto prefix = "hacpp_retain_" + suffix;

  if (shared) {
    auto connection = SharedAsyncMqttConnection::create(AsyncMqttClient{executor, config()});
    co_await require_success(connection->async_connect());
    co_await check_entity<Sensor, SensorCfg>(connection->make_client(), cleanup, prefix + "_sensor");
    co_await check_entity<BinarySensor, BinarySensorCfg>(connection->make_client(), cleanup, prefix + "_binary");
    co_await check_entity<Button, ButtonCfg>(connection->make_client(), cleanup, prefix + "_button");
    co_await check_entity<Cover, CoverCfg>(connection->make_client(), cleanup, prefix + "_cover");
    co_await require_success(connection->async_close());
  } else {
    auto check =
        [&]<template <typename> typename EntityType, typename Cfg>(std::string name) -> boost::asio::awaitable<void> {
      auto client = AsyncMqttClient{executor, config()};
      co_await require_success(client.async_connect());
      co_await check_entity<EntityType, Cfg>(std::move(client), cleanup, prefix + name);
    };
    co_await check.template operator()<Sensor, SensorCfg>("_sensor");
    co_await check.template operator()<BinarySensor, BinarySensorCfg>("_binary");
    co_await check.template operator()<Button, ButtonCfg>("_button");
    co_await check.template operator()<Cover, CoverCfg>("_cover");
  }

  // A non-retained publish before subscription must not be replayed.
  auto topic = prefix + "/raw";
  co_await require_success(cleanup.async_publish(topic, "old", QoS::at_least_once));
  auto verifier = AsyncMqttClient{executor, config()};
  co_await require_success(verifier.async_connect());
  auto topics = std::vector<TopicSubopts>{
      {topic, QoS::at_least_once}
  };
  co_await require_success(verifier.async_subscribe(std::move(topics)));
  co_await require_success(cleanup.async_publish(topic, "live", QoS::at_least_once));
  auto result = co_await verifier.async_recv();
  REQUIRE(result.has_value());
  auto* packet = result->get_if<PublishPacket>();
  REQUIRE(packet);
  REQUIRE(packet->payload() == "live");
  REQUIRE(packet->opts().get_retain() == async_mqtt::pub::retain::no);
  co_await require_success(verifier.async_close());
  co_await require_success(cleanup.async_close());
}
} // namespace

TEST_CASE("Entity configuration and latest state are retained", "[retain]")
{
  bool shared = false;
  SECTION("Direct client")
  {}
  SECTION("Shared client")
  {
    shared = true;
  }

  auto io = boost::asio::io_context{};
  auto strand = boost::asio::make_strand(io);
  auto timer = boost::asio::steady_timer{strand, std::chrono::seconds{30}};
  timer.async_wait([&io](boost::system::error_code err) {
    if (!err) {
      io.stop();
      FAIL("Retain test timed out");
    }
  });
  boost::asio::co_spawn(strand, check_all(strand, shared), [&timer](std::exception_ptr err) {
    timer.cancel();
    if (err) {
      std::rethrow_exception(err);
    }
  });
  io.run();
}
