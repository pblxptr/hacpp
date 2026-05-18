#pragma once

#include <async_mqtt/all.hpp>
#include <cstdint>
#include <expected>
#include <string>

#include <hacpp/error.h>
#include <spdlog/spdlog.h>

namespace hacpp::mqtt {
using Error = boost::system::error_code;

using TopicSubopts = async_mqtt::topic_subopts;
using QoS = async_mqtt::qos;
using RecvResult = std::expected<async_mqtt::packet_variant, Error>;
using PublishPacket = async_mqtt::v5::publish_packet;

// TODO: Use logger instance instead of global spdlog functions, and configure
// it properly (e.g., set log level, format, sinks).

namespace detail {
template <typename T> auto str(const T &value) -> std::string {
  auto ss = std::stringstream{};
  ss << value;
  return ss.str();
}

template <typename T> auto str(const std::optional<T> &opt) -> std::string {
  if (opt) {
    return str(*opt);
  }
  return "nullopt";
}
} // namespace detail

class AsyncMqttClient2 {
  using Impl = async_mqtt::client<async_mqtt::protocol_version::v5,
                                  async_mqtt::protocol::mqtt>;

  enum class State { Closed, Connected, Reconnecting };

  struct Connection {
    State state { State::Closed };
    int attempt = 0;
    int max_attempts = 10;
  };

public:
  struct Config {
    std::string host{"localhost"};
    std::string port{"1883"};
    std::string username{""};
    std::string password{""};
    std::string unique_id{""};
    std::uint16_t keep_alive{0};
    bool clean_start{true};
  };

  AsyncMqttClient2(boost::asio::any_io_executor exe, const Config &config)
      : impl_(Impl{exe}), config_(config) {}

  auto executor() { return impl_.get_executor(); }

  boost::asio::awaitable<Error> async_connect()
  {
    auto err = Error{};
    co_await impl_.async_underlying_handshake(
        config_.host, config_.port,
        boost::asio::redirect_error(boost::asio::use_awaitable, err));

    if (err) {
      spdlog::error("Underlying handshake error: {}", err.message());
      co_return map_err(err);
    }

    auto connack_packet = co_await impl_.async_start(
        config_.clean_start, config_.keep_alive, config_.unique_id,
        std::nullopt, config_.username, config_.password,
        boost::asio::redirect_error(boost::asio::use_awaitable, err));

    if (err) {
      spdlog::error("Connect error: {}", err.message());
      co_return map_err(err);
    }

    conn_.state = State::Connected;
    conn_.attempt = 0;

    spdlog::debug("Connected successfully, connack: {}",
                  detail::str(connack_packet));

    co_return ErrorCode::Success;
  }

  boost::asio::awaitable<Error> async_disconnect() {
    auto err = Error{};
    co_await impl_.async_disconnect(
        boost::asio::redirect_error(boost::asio::use_awaitable, err));

    conn_.state = State::Closed;

    co_return map_err(err);
  }

  boost::asio::awaitable<Error> async_close() {
    auto err = Error{};
    co_await impl_.async_close(
        boost::asio::redirect_error(boost::asio::use_awaitable, err));

    // TODO: Consider calling disconnect first
    conn_.state = State::Closed;

    co_return map_err(err);
  }

  boost::asio::awaitable<Error>
  async_publish(const std::string &topic, const std::string &payload,
                async_mqtt::qos qos = async_mqtt::qos::at_most_once)
  {
    spdlog::debug("Publishing to topic: {}, payload: {}, QoS: {}", topic,
                  payload, static_cast<int>(qos));

    if (conn_.state != State::Connected) {
      co_return ErrorCode::NotConnected;
    }

    auto err = Error{};
    auto pid = qos > QoS::at_most_once
                   ? co_await impl_.async_acquire_unique_packet_id()
                   : static_cast<async_mqtt::basic_packet_id_type<2>::type>(0);
    auto pubres = co_await impl_.async_publish(
        async_mqtt::v5::publish_packet{pid, topic, payload, qos},
        boost::asio::redirect_error(boost::asio::use_awaitable, err));

    spdlog::debug("Publish completed with error code: {} ({})", err.value(),
                  err.message());

    if (err) {
      co_return map_err(err);
    }

    spdlog::debug("Publish result: ");
    if (pubres.puback_opt) {
      spdlog::debug("PubAck: {}", detail::str(pubres.puback_opt));
    }
    if (pubres.pubrec_opt) {
      spdlog::debug("PubRec: {}", detail::str(pubres.pubrec_opt));
    }
    if (pubres.pubcomp_opt) {
      spdlog::debug("PubComp: {}", detail::str(pubres.pubcomp_opt));
    }

    co_return ErrorCode::Success;
  }

  boost::asio::awaitable<Error>
  async_subscribe(const std::vector<TopicSubopts> &sub_entry)
  {
    if (conn_.state != State::Connected) {
      co_return ErrorCode::NotConnected;
    }

    auto err = Error{};
    auto pid_sub = co_await impl_.async_acquire_unique_packet_id();
    auto suback_opt = co_await impl_.async_subscribe(
        pid_sub, sub_entry,
        boost::asio::redirect_error(boost::asio::use_awaitable, err));

    if (err) {
      co_return map_err(err);
    }

    if (suback_opt) {
      spdlog::debug("SubAck: {}", detail::str(suback_opt));
    }

    co_return ErrorCode::Success;
  }

  boost::asio::awaitable<RecvResult> async_recv()
  {
    auto err = Error{};
    auto packet = co_await impl_.async_recv(
        boost::asio::redirect_error(boost::asio::use_awaitable, err));

    if (err) {
      auto err_rc = co_await async_handle_reconnect();
      co_return std::unexpected(err_rc);
    }
    if (err) {
      co_return std::unexpected(map_err(err));
    }


    packet->visit(
        [](auto &&p) { spdlog::debug("Received packet: {}", detail::str(p)); });

    co_return RecvResult{*packet};
  }

private:
  boost::asio::awaitable<Error> async_handle_reconnect()
  {
    if (conn_.state == State::Closed) {
      co_return ErrorCode::Disconnected;
    }
    // TODO: Disable reconnection when user expliclity calls close/disconnect

    const auto base_delay = std::chrono::seconds{1};
    auto timer = boost::asio::steady_timer{executor()};

    if (conn_.attempt >= conn_.max_attempts) {
      co_return ErrorCode::Disconnected;
    }

    conn_.state = State::Reconnecting;

    auto err = Error{};
    while (conn_.attempt++ < conn_.max_attempts) {
      spdlog::debug("Reconnecting, attempt: {}/{}", conn_.attempt,
                    conn_.max_attempts);

      // timer.expires_after(std::chrono::seconds{base_delay.count() * std::pow(2, conn_.attempt - 1)});
      timer.expires_after(std::chrono::seconds{base_delay.count() * static_cast<int>(std::pow(2, conn_.attempt - 1))});
      co_await timer.async_wait(boost::asio::use_awaitable);

      err = co_await async_connect();
      if (!err) {
        co_return ErrorCode::Reconnected;
      }

      spdlog::debug("Reconnecting failed: {}", err.message());
    }
    co_return err;
  }

private:
  Impl impl_;
  Config config_;
  Connection conn_;
};

using ClientType = AsyncMqttClient2;

} // namespace hacpp::mqtt
