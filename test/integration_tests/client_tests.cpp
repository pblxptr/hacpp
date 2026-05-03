#include <hacpp/async_mqtt_client.h>

#include <catch2/catch_all.hpp>

#include "config.h"

using hacpp::mqtt::Error;
using hacpp::mqtt::ErrorCode;

TEST_CASE("Client can connect to broker")
{
    // Arrange
    auto io = boost::asio::io_context{};
    auto strand = boost::asio::make_strand(io);
    auto client = hacpp::mqtt::AsyncMqttClient2{strand, config};

    // NOLINTBEGIN
    boost::asio::co_spawn(
        strand,
        [client = std::move(client)]() mutable -> boost::asio::awaitable<void> {
            // Act
            auto err = co_await client.async_connect();

            // Assert
            REQUIRE(!err);
            co_await client.async_close();
        }, rethrow);
    // NOLINTEND

    io.run();
}

TEST_CASE("Client cannot connect to broker") {
    // Arrange
    auto io = boost::asio::io_context{};
    auto strand = boost::asio::make_strand(io);
    SECTION("when providing invalid credentials") {
        // Arrange
        auto invalid_config = config;
        invalid_config.password = "invalid_password";
        auto client = hacpp::mqtt::AsyncMqttClient2{strand, invalid_config};
        // NOLINTBEGIN
        boost::asio::co_spawn(
            strand,
            [client = std::move(client)]() mutable -> boost::asio::awaitable<void> {
                // Act
                auto err = co_await client.async_connect();

                // Assert
                REQUIRE(err);
                REQUIRE(err == ErrorCode::NotAuthorized);
                co_await client.async_close();
            }, rethrow);
        // NOLINTEND
    }

    SECTION("when host is unavailable") {
        // Arrange
        auto unavailable_config = config;
        unavailable_config.host = "invalid_host";
        auto client = hacpp::mqtt::AsyncMqttClient2{strand, unavailable_config};
        // NOLINTBEGIN
        boost::asio::co_spawn(
            strand,
            [client = std::move(client)]() mutable -> boost::asio::awaitable<void> {
                // Act
                auto err = co_await client.async_connect();

                // Assert
                REQUIRE(err);
                REQUIRE(err == ErrorCode::HostNotFound);
                co_await client.async_close();
            }, rethrow);
        // NOLINTEND
    }

    SECTION("when port is unavailable") {
        // Arrange
        auto unavailable_config = config;
        unavailable_config.port = "9999";
        auto client = hacpp::mqtt::AsyncMqttClient2{strand, unavailable_config};
        // NOLINTBEGIN
        boost::asio::co_spawn(
            strand,
            [client = std::move(client)]() mutable -> boost::asio::awaitable<void> {
                // Act
                auto err = co_await client.async_connect();

                // Assert
                REQUIRE(err);
                REQUIRE(err == ErrorCode::ConnectionRefused);
                co_await client.async_close();
            }, rethrow);
        // NOLINTEND
    }

}

TEST_CASE("Client is not operational when disconnected") {
    // Arrange
    auto io = boost::asio::io_context{};
    auto strand = boost::asio::make_strand(io);
    auto client = hacpp::mqtt::AsyncMqttClient2{strand, config};

    SECTION("cannot publish") {
        boost::asio::co_spawn(
            strand,
            [client = std::move(client)]() mutable -> boost::asio::awaitable<void> {
                // Act
                auto err = co_await client.async_publish("test/topic", "payload");

                // Assert
                REQUIRE(err);
                REQUIRE(err == ErrorCode::PacketNotAllowedToSend);
                co_await client.async_close();
            }, rethrow);
    }

    SECTION("cannot subscribe") {
        boost::asio::co_spawn(
            strand,
            [client = std::move(client)]() mutable -> boost::asio::awaitable<void> {
                // Act
                auto topics = std::vector<hacpp::mqtt::TopicSubopts>{
                    {"test/topic", async_mqtt::qos::at_most_once}
                };
                auto err = co_await client.async_subscribe(topics);

                // Assert
                REQUIRE(err);
                REQUIRE(err == ErrorCode::PacketNotAllowedToSend);
                co_await client.async_close();
            }, rethrow);
    }

    io.run();
}
