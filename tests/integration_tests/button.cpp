#include <catch2/catch_all.hpp>
#include <hacpp/button.h>

#include "config.h"

using namespace hacpp::mqtt;

constexpr static auto UniqueId = "button_unique_id";

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
        { default_component_discovery_topic(Button::Defs::Component, UniqueId), QoS::at_most_once },
        { default_component_command_topic(Button::Defs::Component, UniqueId), QoS::at_most_once },
        { default_component_availability_topic(Button::Defs::Component, UniqueId), QoS::at_most_once }
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

TEST_CASE("Button provides all required options during discovery")
{
    // Arrange
    auto io = boost::asio::io_context{};
    auto strand = boost::asio::make_strand(io);
    boost::asio::co_spawn(strand, [&]() mutable -> boost::asio::awaitable<void> {
        auto entity_client = co_await get_client(strand);
        auto verifier_client = co_await get_verifier(strand);
        auto button = Factory<Button>(UniqueId, std::move(entity_client))
            .create();

        // Act
        auto err = co_await button.async_discovery();
        auto packet = co_await async_recv_packet<PublishPacket>(verifier_client);

        // Assert
        REQUIRE(!err);
        REQUIRE(packet.topic() == default_component_discovery_topic(Button::Defs::Component, UniqueId));
        auto pobj = boost::json::parse(packet.payload());
        REQUIRE(pobj.as_object().contains(Button::Opt::CommandTopic.key));
        REQUIRE(!pobj.as_object()[Button::Opt::CommandTopic.key].as_string().empty());

        co_await button.async_close();
        co_await verifier_client.async_close();
    }, rethrow);

    io.run();
}

TEST_CASE("Button can receive press command")
{
    // Arrange
    auto io = boost::asio::io_context{};
    auto strand = boost::asio::make_strand(io);

    boost::asio::co_spawn(strand, [&]() mutable -> boost::asio::awaitable<void> {
        auto entity_client = co_await get_client(strand);
        auto verifier_client = co_await get_verifier(strand);

        bool pressed = false;
        auto button = Factory<Button>(UniqueId, std::move(entity_client))
            .on_press([&pressed]() -> boost::asio::awaitable<void> {
                pressed = true;
                co_return;
            })
            .create();

        auto err_disc = co_await button.async_discovery();
        REQUIRE(!err_disc);
        auto packet_disc = co_await async_recv_packet<PublishPacket>(verifier_client);

        // Start button loop in background
        boost::asio::co_spawn(strand, [&]() -> boost::asio::awaitable<void> {
            co_await button.async_run();
        }, rethrow);

        // Act
        auto err_pub = co_await verifier_client.async_publish(
            default_component_command_topic(Button::Defs::Component, UniqueId),
            Button::Defs::PayloadPress
        );
        REQUIRE(!err_pub);

        // Give some time for packet delivery and processing
        auto timer = boost::asio::steady_timer{strand};
        timer.expires_after(std::chrono::milliseconds(100));
        co_await timer.async_wait(boost::asio::use_awaitable);

        // Assert
        REQUIRE(pressed);

        co_await button.async_close();
        co_await verifier_client.async_close();
    }, rethrow);

    io.run();
}

TEST_CASE("Button availability")
{
    // Arrange
    auto io = boost::asio::io_context{};
    auto strand = boost::asio::make_strand(io);

    boost::asio::co_spawn(strand, [&]() mutable -> boost::asio::awaitable<void> {
        auto entity_client = co_await get_client(strand);
        auto verifier_client = co_await get_verifier(strand);
        auto button = Factory<Button>(UniqueId, std::move(entity_client))
            .set(Availability::Opt::Topic, default_component_availability_topic(Button::Defs::Component, UniqueId))
            .create();

        auto err_disc = co_await button.async_discovery();
        REQUIRE(!err_disc);
        auto packet_disc = co_await async_recv_packet<PublishPacket>(verifier_client);

        SECTION("provides 'online' state") {
            // Act
            auto err = co_await button.async_update_availability(true);
            auto packet = co_await async_recv_packet<PublishPacket>(verifier_client);

            // Assert
            REQUIRE(!err);
            REQUIRE(packet.topic() == default_component_availability_topic(Button::Defs::Component, UniqueId));
            REQUIRE(packet.payload() == Availability::Defs::PayloadAvailable);
        }

        SECTION("provides 'offline' state") {
            // Act
            auto err = co_await button.async_update_availability(false);
            auto packet = co_await async_recv_packet<PublishPacket>(verifier_client);

            // Assert
            REQUIRE(!err);
            REQUIRE(packet.topic() == default_component_availability_topic(Button::Defs::Component, UniqueId));
            REQUIRE(packet.payload() == Availability::Defs::PayloadNotAvailable);
        }

        co_await button.async_close();
        co_await verifier_client.async_close();
    }, rethrow);

    io.run();
}
