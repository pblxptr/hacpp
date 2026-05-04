#include <catch2/catch_all.hpp>
#include <hacpp/binary_sensor.h>

#include "config.h"

using namespace hacpp::mqtt;

constexpr static auto UniqueId = "binary_sensor_unique_id";

static boost::asio::awaitable<ClientType> get_client(boost::asio::any_io_executor exe)
{
    auto client = ClientType{exe, config};
    auto err = co_await client.async_connect();
    REQUIRE(!err);

    co_return client;
}

static boost::asio::awaitable<ClientType> get_verifier(boost::asio::any_io_executor exe)
{
    auto client = ClientType{exe, config};
    auto err = co_await client.async_connect();
    REQUIRE(!err);

    auto sub_topics = std::vector<TopicSubopts>{
        { default_component_discovery_topic(BinarySensor::Defs::Component, UniqueId), QoS::at_most_once },
        { default_component_state_topic(BinarySensor::Defs::Component, UniqueId), QoS::at_most_once },
        { default_component_availability_topic(BinarySensor::Defs::Component, UniqueId), QoS::at_most_once }
    };

    err = co_await client.async_subscribe(sub_topics);
    REQUIRE(!err);

    co_return client;
}

template <typename T>
boost::asio::awaitable<T> async_recv_packet(ClientType& client)
{
    auto res = co_await client.async_recv();
    REQUIRE(res.has_value());

    auto* packet = res->template get_if<T>();
    REQUIRE(packet);

    co_return *packet;
}

TEST_CASE("Binary sensor provides all required options during discovery")
{
    // Arrange
    auto io = boost::asio::io_context{};
    auto strand = boost::asio::make_strand(io);
    boost::asio::co_spawn(strand, [&strand]() mutable -> boost::asio::awaitable<void> {
        auto entity_client = co_await get_client(strand);
        auto verifier_client = co_await get_verifier(strand);
        auto binary_sensor = Factory<BinarySensor>(UniqueId, std::move(entity_client))
            .create();

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
        co_await verifier_client.async_close();
    }, rethrow);

    io.run();
}

TEST_CASE("Binary sensor can update its state")
{
    // Arrange
    auto io = boost::asio::io_context{};
    auto strand = boost::asio::make_strand(io);

    boost::asio::co_spawn(strand, [&strand]() mutable -> boost::asio::awaitable<void> {
        auto entity_client = co_await get_client(strand);
        auto verifier_client = co_await get_verifier(strand);
        auto binary_sensor = Factory<BinarySensor>(UniqueId, std::move(entity_client))
            .set(Availability::Opt::Topic, default_component_availability_topic(BinarySensor::Defs::Component, UniqueId))
            .create();
        auto err1 = co_await binary_sensor.async_discovery();
        auto packet = co_await async_recv_packet<PublishPacket>(verifier_client);
        REQUIRE(!err1);

        SECTION("provides 'on' state") {
            // Act
            auto err = co_await binary_sensor.async_update_state(true);
            auto packet = co_await async_recv_packet<PublishPacket>(verifier_client);

            // Assert
            REQUIRE(!err);
            REQUIRE(packet.topic() == binary_sensor.config().at(BinarySensor::Opt::StateTopic));
            REQUIRE(packet.payload() == BinarySensor::Defs::PayloadOn);
            co_await binary_sensor.async_close();
            co_await verifier_client.async_close();
        }

        SECTION("provides 'off' state") {
            // Act
            auto err = co_await binary_sensor.async_update_state(false);
            auto packet = co_await async_recv_packet<PublishPacket>(verifier_client);

            // Assert
            REQUIRE(!err);
            REQUIRE(packet.topic() == binary_sensor.config().at(BinarySensor::Opt::StateTopic));
            REQUIRE(packet.payload() == BinarySensor::Defs::PayloadOff);
            co_await binary_sensor.async_close();
            co_await verifier_client.async_close();
        }
    }, rethrow);

    io.run();
}

TEST_CASE("Binary sensor can update its availability")
{
    // Arrange
    auto io = boost::asio::io_context{};
    auto strand = boost::asio::make_strand(io);

    boost::asio::co_spawn(strand, [&strand]() mutable -> boost::asio::awaitable<void> {
        auto entity_client = co_await get_client(strand);
        auto verifier_client = co_await get_verifier(strand);
        auto binary_sensor = Factory<BinarySensor>(UniqueId, std::move(entity_client))
            .set(Availability::Opt::Topic, default_component_availability_topic(BinarySensor::Defs::Component, UniqueId))
            .create();

        auto err_disc = co_await binary_sensor.async_discovery();
        REQUIRE(!err_disc);
        auto packet_disc = co_await async_recv_packet<PublishPacket>(verifier_client);

        SECTION("provides 'online' state") {
            // Act
            auto err = co_await binary_sensor.async_update_availability(true);
            auto packet = co_await async_recv_packet<PublishPacket>(verifier_client);

            // Assert
            REQUIRE(!err);
            REQUIRE(packet.topic() == default_component_availability_topic(BinarySensor::Defs::Component, UniqueId));
            REQUIRE(packet.payload() == Availability::Defs::PayloadAvailable);
        }

        SECTION("provides 'offline' state") {
            // Act
            auto err = co_await binary_sensor.async_update_availability(false);
            auto packet = co_await async_recv_packet<PublishPacket>(verifier_client);

            // Assert
            REQUIRE(!err);
            REQUIRE(packet.topic() == default_component_availability_topic(BinarySensor::Defs::Component, UniqueId));
            REQUIRE(packet.payload() == Availability::Defs::PayloadNotAvailable);
        }

        co_await binary_sensor.async_close();
        co_await verifier_client.async_close();
    }, rethrow);

    io.run();
}
