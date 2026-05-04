#include <catch2/catch_all.hpp>
#include <hacpp/sensor.h>

#include "config.h"

using namespace hacpp::mqtt;

constexpr static auto UniqueId = "sensor_unique_id";


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
        { default_component_discovery_topic(Sensor::Defs::Component, UniqueId), QoS::at_most_once },
        { default_component_state_topic(Sensor::Defs::Component, UniqueId), QoS::at_most_once },
        { default_component_availability_topic(Sensor::Defs::Component, UniqueId), QoS::at_most_once }
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

TEST_CASE("Sensor provides all required options during discovery")
{
    // Arrange
    auto io = boost::asio::io_context{};
    auto strand = boost::asio::make_strand(io);
    boost::asio::co_spawn(strand, [&strand]() mutable -> boost::asio::awaitable<void> {
        auto entity_client = co_await get_client(strand);
        auto verifier_client = co_await get_verifier(strand);
        auto sensor = Factory<Sensor>(UniqueId, std::move(entity_client))
            .create();

        // Act
        auto err = co_await sensor.async_discovery();
        auto packet = co_await async_recv_packet<PublishPacket>(verifier_client);

        // Assert
        REQUIRE(!err);
        REQUIRE(packet.topic() == default_component_discovery_topic(Sensor::Defs::Component, UniqueId));
        auto pobj = boost::json::parse(packet.payload());
        REQUIRE(pobj.as_object().contains(Sensor::Opt::StateTopic.key));
        REQUIRE(!pobj.as_object()[Sensor::Opt::StateTopic.key].as_string().empty());

        co_await sensor.async_close();
        co_await verifier_client.async_close();
    }, rethrow);

    io.run();
}

TEST_CASE("Sensor can update its state")
{
    // Arrange
    auto io = boost::asio::io_context{};
    auto strand = boost::asio::make_strand(io);

    boost::asio::co_spawn(strand, [&strand]() mutable -> boost::asio::awaitable<void> {
        auto entity_client = co_await get_client(strand);
        auto verifier_client = co_await get_verifier(strand);
        auto sensor = Factory<Sensor>(UniqueId, std::move(entity_client))
            .create();
        auto err1 = co_await sensor.async_discovery();
        auto packet1 = co_await async_recv_packet<PublishPacket>(verifier_client);
        REQUIRE(!err1);

        // Act
        auto err = co_await sensor.async_update_state("12.5");
        auto packet = co_await async_recv_packet<PublishPacket>(verifier_client);

        // Assert
        REQUIRE(!err);
        REQUIRE(packet.topic() == sensor.config().at(Sensor::Opt::StateTopic));
        REQUIRE(packet.payload() == "12.5");

        co_await sensor.async_close();
        co_await verifier_client.async_close();
    }, rethrow);

    io.run();
}

TEST_CASE("Sensor availability")
{
    // Arrange
    auto io = boost::asio::io_context{};
    auto strand = boost::asio::make_strand(io);

    boost::asio::co_spawn(strand, [&strand]() mutable -> boost::asio::awaitable<void> {
        auto entity_client = co_await get_client(strand);
        auto verifier_client = co_await get_verifier(strand);
        auto sensor = Factory<Sensor>(UniqueId, std::move(entity_client))
            .set(Availability::Opt::Topic, default_component_availability_topic(Sensor::Defs::Component, UniqueId))
            .create();

        auto err_disc = co_await sensor.async_discovery();
        REQUIRE(!err_disc);
        auto packet_disc = co_await async_recv_packet<PublishPacket>(verifier_client);

        SECTION("provides 'online' state") {
            // Act
            auto err = co_await sensor.async_update_availability(true);
            auto packet = co_await async_recv_packet<PublishPacket>(verifier_client);

            // Assert
            REQUIRE(!err);
            REQUIRE(packet.topic() == default_component_availability_topic(Sensor::Defs::Component, UniqueId));
            REQUIRE(packet.payload() == Availability::Defs::PayloadAvailable);
        }

        SECTION("provides 'offline' state") {
            // Act
            auto err = co_await sensor.async_update_availability(false);
            auto packet = co_await async_recv_packet<PublishPacket>(verifier_client);

            // Assert
            REQUIRE(!err);
            REQUIRE(packet.topic() == default_component_availability_topic(Sensor::Defs::Component, UniqueId));
            REQUIRE(packet.payload() == Availability::Defs::PayloadNotAvailable);
        }

        co_await sensor.async_close();
        co_await verifier_client.async_close();
    }, rethrow);

    io.run();
}
