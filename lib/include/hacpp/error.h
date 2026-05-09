#pragma once

#include <async_mqtt/protocol/error.hpp>
#include <boost/system/error_code.hpp>
#include <spdlog/spdlog.h>
#include <string>

namespace hacpp::mqtt {

enum class ErrorCode {
  Success = 0,
  NotAuthorized,
  HostNotFound,
  ConnectionRefused,
  PacketNotAllowedToSend, // TODO: Perhaps too specific?
  NotConnected,
  InvalidConfig,
  UnknownError
};

inline const boost::system::error_category &error_category();

inline boost::system::error_code make_error_code(ErrorCode e);

} // namespace hacpp::mqtt

namespace boost::system {
template <>
struct is_error_code_enum<hacpp::mqtt::ErrorCode> : std::true_type {};
} // namespace boost::system

namespace hacpp::mqtt {

class ErrorCategory : public boost::system::error_category {
public:
  const char *name() const noexcept override { return "hacpp::mqtt"; }

  std::string message(int ev) const override {
    switch (static_cast<ErrorCode>(ev)) {
    case ErrorCode::Success:
      return "success";
    case ErrorCode::NotAuthorized:
      return "not_authorized";
    case ErrorCode::HostNotFound:
      return "host_not_found";
    case ErrorCode::ConnectionRefused:
      return "connection_refused";
    case ErrorCode::PacketNotAllowedToSend:
      return "packet_not_allowed_to_send";
    case ErrorCode::NotConnected:
      return "not_connected";
    case ErrorCode::InvalidConfig:
      return "invalid_config";
    case ErrorCode::UnknownError:
      return "unknown_error";
    default:
      return "unknown_error";
    }
  }
};

inline const boost::system::error_category &error_category() {
  static ErrorCategory instance;
  return instance;
}

inline boost::system::error_code make_error_code(ErrorCode e) {
  return {static_cast<int>(e), error_category()};
}

namespace detail {
inline ErrorCode map_connect_error(int ev) {
  switch (static_cast<async_mqtt::connect_reason_code>(ev)) {
  case async_mqtt::connect_reason_code::not_authorized:
    return ErrorCode::NotAuthorized;
  default:
    return ErrorCode::UnknownError;
  }
}

inline ErrorCode map_netdb_error(int ev) {
  switch (ev) {
  case boost::asio::error::host_not_found:
  case boost::asio::error::host_not_found_try_again:
    return ErrorCode::HostNotFound;
  default:
    return ErrorCode::UnknownError;
  }
}

inline ErrorCode map_system_error(int ev) {
  switch (ev) {
  case boost::asio::error::connection_refused:
    return ErrorCode::ConnectionRefused;
  case boost::asio::error::not_connected:
    return ErrorCode::NotConnected;
  default:
    return ErrorCode::UnknownError;
  }
}

inline ErrorCode map_mqtt_error(int ev) {
  switch (static_cast<async_mqtt::mqtt_error>(ev)) {
  case async_mqtt::mqtt_error::packet_not_allowed_to_send:
    return ErrorCode::PacketNotAllowedToSend;
  default:
    return ErrorCode::UnknownError;
  }
}
} // namespace detail

inline boost::system::error_code map_err(const boost::system::error_code &ec) {
  if (!ec) {
    return ErrorCode::Success;
  }

  if (ec.category() == async_mqtt::get_connect_reason_code_category()) {
    return detail::map_connect_error(ec.value());
  }

  if (ec.category() == boost::asio::error::get_netdb_category()) {
    return detail::map_netdb_error(ec.value());
  }

  if (ec.category() == boost::asio::error::get_system_category()) {
    return detail::map_system_error(ec.value());
  }

  if (ec.category() == async_mqtt::get_mqtt_error_category()) {
    return detail::map_mqtt_error(ec.value());
  }

  spdlog::warn("Unknown error category: {} ({}), err: {}", ec.category().name(),
               ec.value(), ec.message());

  return ErrorCode::UnknownError;
}

} // namespace hacpp::mqtt
