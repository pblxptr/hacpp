#include <catch2/catch_all.hpp>
#include <hacpp/cover.h>

#include "config.h"

using namespace hacpp::mqtt;

constexpr static auto UniqueId = "cover_unique_id";

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
        { default_component_discovery_topic(Cover::Defs::Component, UniqueId), QoS::at_most_once },
        { default_component_command_topic(Cover::Defs::Component, UniqueId), QoS::at_most_once },
        { default_component_state_topic(Cover::Defs::Component, UniqueId), QoS::at_most_once },
        { default_component_availability_topic(Cover::Defs::Component, UniqueId), QoS::at_most_once }
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

TEST_CASE("Cover provides all required options during discovery")
{
    // Arrange
    auto io = boost::asio::io_context{};
    auto strand = boost::asio::make_strand(io);
    boost::asio::co_spawn(strand, [&strand]() mutable -> boost::asio::awaitable<void> {
        auto entity_client = co_await get_client(strand);
        auto verifier_client = co_await get_verifier(strand);
        auto cover = Factory<Cover>(UniqueId, std::move(entity_client))
            .create();

        // Act
        auto err = co_await cover.async_discovery();
        auto packet = co_await async_recv_packet<PublishPacket>(verifier_client);

        // Assert
        REQUIRE(!err);
        REQUIRE(packet.topic() == default_component_discovery_topic(Cover::Defs::Component, UniqueId));
        auto pobj = boost::json::parse(packet.payload());
        REQUIRE(pobj.as_object().contains(Cover::Opt::CommandTopic.key));
        REQUIRE(!pobj.as_object()[Cover::Opt::CommandTopic.key].as_string().empty());

        co_await cover.async_close();
        co_await verifier_client.async_close();
    }, rethrow);

    io.run();
}

TEST_CASE("Cover can receive commands")
{
    // Arrange
    auto io = boost::asio::io_context{};
    auto strand = boost::asio::make_strand(io);

    boost::asio::co_spawn(strand, [&strand]() mutable -> boost::asio::awaitable<void> {
        auto entity_client = co_await get_client(strand);
        auto verifier_client = co_await get_verifier(strand);

        std::string received_command;
        auto cover = Factory<Cover>(UniqueId, std::move(entity_client))
            .on_open([&]() -> boost::asio::awaitable<void> {
                received_command = "OPEN";
                co_return;
            })
            .on_close([&]() -> boost::asio::awaitable<void> {
                received_command = "CLOSE";
                co_return;
            })
            .on_stop([&]() -> boost::asio::awaitable<void> {
                received_command = "STOP";
                co_return;
            })
            .create();

        auto err_disc = co_await cover.async_discovery();
        REQUIRE(!err_disc);
        auto packet_disc = co_await async_recv_packet<PublishPacket>(verifier_client);

        // Start cover loop in background
        boost::asio::co_spawn(strand, [&]() -> boost::asio::awaitable<void> {
            co_await cover.async_run();
        }, rethrow);

        SECTION("OPEN command") {
            auto err_pub = co_await verifier_client.async_publish(
                default_component_command_topic(Cover::Defs::Component, UniqueId),
                Cover::Defs::PayloadOpen
            );
            REQUIRE(!err_pub);

            auto timer = boost::asio::steady_timer{strand};
            timer.expires_after(std::chrono::milliseconds(100));
            co_await timer.async_wait(boost::asio::use_awaitable);
            REQUIRE(received_command == "OPEN");
        }

        SECTION("CLOSE command") {
            auto err_pub = co_await verifier_client.async_publish(
                default_component_command_topic(Cover::Defs::Component, UniqueId),
                Cover::Defs::PayloadClose
            );
            REQUIRE(!err_pub);

            auto timer = boost::asio::steady_timer{strand};
            timer.expires_after(std::chrono::milliseconds(100));
            co_await timer.async_wait(boost::asio::use_awaitable);
            REQUIRE(received_command == "CLOSE");
        }

        SECTION("STOP command") {
            auto err_pub = co_await verifier_client.async_publish(
                default_component_command_topic(Cover::Defs::Component, UniqueId),
                Cover::Defs::PayloadStop
            );
            REQUIRE(!err_pub);

            auto timer = boost::asio::steady_timer{strand};
            timer.expires_after(std::chrono::milliseconds(100));
            co_await timer.async_wait(boost::asio::use_awaitable);
            REQUIRE(received_command == "STOP");
        }

        co_await cover.async_close();
        co_await verifier_client.async_close();
    }, rethrow);

    io.run();
}

TEST_CASE("Cover state update")
{
    // Arrange
    auto io = boost::asio::io_context{};
    auto strand = boost::asio::make_strand(io);

    boost::asio::co_spawn(strand, [&strand]() mutable -> boost::asio::awaitable<void> {
        auto entity_client = co_await get_client(strand);
        auto verifier_client = co_await get_verifier(strand);
        auto cover = Factory<Cover>(UniqueId, std::move(entity_client))
            .set(Cover::Opt::StateTopic, default_component_state_topic(Cover::Defs::Component, UniqueId))
            .create();

        auto err_disc = co_await cover.async_discovery();
        REQUIRE(!err_disc);
        auto packet_disc = co_await async_recv_packet<PublishPacket>(verifier_client);

        // Act
        auto err = co_await cover.async_update_state(Cover::Defs::StateOpen);
        auto packet = co_await async_recv_packet<PublishPacket>(verifier_client);

        // Assert
        REQUIRE(!err);
        REQUIRE(packet.topic() == default_component_state_topic(Cover::Defs::Component, UniqueId));
        REQUIRE(packet.payload() == Cover::Defs::StateOpen);

        co_await cover.async_close();
        co_await verifier_client.async_close();
    }, rethrow);

    io.run();
}
