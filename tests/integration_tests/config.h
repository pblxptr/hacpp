#pragma once

#include <hacpp/async_mqtt_client.h>

#include <boost/asio.hpp>

static inline const auto config = hacpp::mqtt::AsyncMqttClient2::Config{
    .host = "localhost",
    .port = "1883",
    .username = "test_user",
    .password = "test",
    .keep_alive = 0,
    .clean_start = true};

inline auto rethrow(const std::exception_ptr& eptr)
{
  if (eptr) {
    std::rethrow_exception(eptr);
  }
}
