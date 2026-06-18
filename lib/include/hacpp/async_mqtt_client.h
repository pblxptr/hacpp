#pragma once

#include <async_mqtt/all.hpp>
#include <hacpp/error.h>

#include <spdlog/spdlog.h>

#include <cstdint>
#include <expected>
#include <optional>
#include <sstream>
#include <string>
#include <utility>

namespace hacpp::mqtt {
using Error = boost::system::error_code;

using TopicSubopts = async_mqtt::topic_subopts;
using QoS = async_mqtt::qos;
using RecvResult = std::expected<async_mqtt::packet_variant, Error>;
using PublishPacket = async_mqtt::v5::publish_packet;

// TODO(pbiel): Use logger instance instead of global spdlog functions, and configure
// it properly (e.g., set log level, format, sinks).

namespace detail {
template <typename T>
auto str(const T& value) -> std::string
{
  auto ss = std::stringstream{};
  ss << value;
  return ss.str();
}

template <typename T>
auto str(const std::optional<T>& opt) -> std::string
{
  if (opt) {
    return str(*opt);
  }
  return "nullopt";
}
} // namespace detail

class AsyncMqttClient2
{
    using Impl = async_mqtt::client<async_mqtt::protocol_version::v5, async_mqtt::protocol::mqtt>;

    enum class State
    {
      Closed,
      Connected,
      Reconnecting
    };

    struct Connection
    {
        static constexpr auto DefaultMaxAutoreconnectAttemps = 10;

        explicit Connection(const boost::asio::any_io_executor& exe)
            : autorec_wait_timer{exe}
        {}

        State state{State::Closed};
        int attempt{0};
        int max_attempts{DefaultMaxAutoreconnectAttemps};
        boost::asio::steady_timer autorec_wait_timer;
    };

  public:
    struct Config
    {
        std::string host{"localhost"};
        std::string port{"1883"};
        std::string username;
        std::string password;
        std::string unique_id;
        std::uint16_t keep_alive{0};
        bool clean_start{true};
    };

    AsyncMqttClient2(const boost::asio::any_io_executor& exe, Config config)
        : impl_{exe}
        , config_{std::move(config)}
        , conn_{exe}
    {}

    auto executor()
    {
      return impl_.get_executor();
    }

    boost::asio::awaitable<Error> async_connect()
    {
      auto err = Error{};
      co_await impl_.async_underlying_handshake(
          config_.host,
          config_.port,
          boost::asio::redirect_error(boost::asio::use_awaitable, err));

      if (err) {
        spdlog::error("Underlying handshake error: {}", err.message());
        co_return map_err(err);
      }

      auto connack_packet = co_await impl_.async_start(
          config_.clean_start,
          config_.keep_alive,
          config_.unique_id,
          std::nullopt,
          config_.username,
          config_.password,
          boost::asio::redirect_error(boost::asio::use_awaitable, err));

      if (err) {
        spdlog::error("Connect error: {}", err.message());
        co_return map_err(err);
      }

      conn_.state = State::Connected;
      conn_.attempt = 0;

      spdlog::debug("Connected successfully, connack: {}", detail::str(connack_packet));

      co_return ErrorCode::Success;
    }

    boost::asio::awaitable<Error> async_disconnect()
    {
      auto err = Error{};
      co_await impl_.async_disconnect(boost::asio::redirect_error(boost::asio::use_awaitable, err));

      conn_.state = State::Closed;
      conn_.autorec_wait_timer.cancel();

      co_return map_err(err);
    }

    boost::asio::awaitable<Error> async_close()
    {
      auto err = Error{};
      co_await impl_.async_close(boost::asio::redirect_error(boost::asio::use_awaitable, err));

      // TODO(pbiel): Consider calling disconnect first
      conn_.state = State::Closed;
      conn_.autorec_wait_timer.cancel();

      co_return map_err(err);
    }

    boost::asio::awaitable<Error>
    async_publish(std::string topic, std::string payload, async_mqtt::qos qos = async_mqtt::qos::at_most_once)
    {
      spdlog::debug("Publishing to topic: {}, payload: {}, QoS: {}", topic, payload, static_cast<int>(qos));

      if (conn_.state == State::Reconnecting) {
        co_await async_wait_autoreconnect();
      }

      if (conn_.state != State::Connected) {
        spdlog::warn("Not connected, cannot publish");
        co_return ErrorCode::NotConnected;
      }

      auto err = Error{};
      auto pid = qos > QoS::at_most_once ? co_await impl_.async_acquire_unique_packet_id()
                                         : static_cast<async_mqtt::basic_packet_id_type<2>::type>(0);
      auto pubres = co_await impl_.async_publish(
          async_mqtt::v5::publish_packet{pid, std::move(topic), std::move(payload), qos},
          boost::asio::redirect_error(boost::asio::use_awaitable, err));

      spdlog::debug("Publish completed with error code: {} ({})", err.value(), err.message());

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

    boost::asio::awaitable<Error> async_subscribe(std::vector<TopicSubopts> sub_entry)
    {
      if (conn_.state == State::Reconnecting) {
        co_await async_wait_autoreconnect();
      }

      if (conn_.state != State::Connected) {
        spdlog::warn("Not connected, cannot subscribe");
        co_return ErrorCode::NotConnected;
      }

      auto err = Error{};
      auto pid_sub = co_await impl_.async_acquire_unique_packet_id();
      auto suback_opt = co_await impl_.async_subscribe(
          pid_sub,
          std::move(sub_entry),
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
      auto packet = co_await impl_.async_recv(boost::asio::redirect_error(boost::asio::use_awaitable, err));

      if (err) {
        auto err_rc = co_await async_handle_reconnect();
        co_return std::unexpected(err_rc);
      }

      if (!packet) {
        co_return std::unexpected(ErrorCode::InvalidPacket);
      }

      packet->visit([](auto&& p) { spdlog::debug("Received packet: {}", detail::str(p)); });

      co_return RecvResult{*packet};
    }

  private:
    boost::asio::awaitable<void> async_wait_autoreconnect()
    {
      auto err = Error{};
      co_await conn_.autorec_wait_timer.async_wait(boost::asio::redirect_error(boost::asio::use_awaitable, err));

      if (err == boost::asio::error::operation_aborted) {
        spdlog::debug("Reconnect wait finished by notification");
      } else if (err) {
        spdlog::warn("Reconnect wait failed: {}", err.message());
      } else {
        spdlog::warn("Reconnect wait timer expired unexpectedly");
      }
    }

    boost::asio::awaitable<Error> async_handle_reconnect()
    {
      if (conn_.state == State::Closed) {
        co_return ErrorCode::Disconnected;
      }

      if (conn_.state == State::Reconnecting) {
        spdlog::warn("Already reconnecting, cannot handle another reconnect");
        co_return ErrorCode::InternalError;
      }

      auto delay = std::chrono::seconds{1};
      const auto max_delay = std::chrono::seconds{30};
      auto timer = boost::asio::steady_timer{executor()};

      conn_.state = State::Reconnecting;
      conn_.autorec_wait_timer.expires_at(boost::asio::steady_timer::time_point::max());

      auto err = Error{};
      while (conn_.attempt++ < conn_.max_attempts) {
        spdlog::debug("Reconnecting, attempt: {}/{}", conn_.attempt, conn_.max_attempts);

        timer.expires_after(std::chrono::seconds{delay});
        co_await timer.async_wait(boost::asio::use_awaitable);

        delay = std::min(delay * 2, max_delay);

        err = co_await async_connect();
        if (!err) {
          conn_.state = State::Connected;
          conn_.autorec_wait_timer.cancel();
          spdlog::debug("Reconnection successful");
          co_return ErrorCode::SessionLost;
        }

        spdlog::debug("Reconnecting failed: {}", err.message());
      }

      conn_.state = State::Closed;
      conn_.autorec_wait_timer.cancel();

      co_return err;
    }

  private:
    Impl impl_;
    Config config_;
    Connection conn_;
};

using ClientType = AsyncMqttClient2;

} // namespace hacpp::mqtt
