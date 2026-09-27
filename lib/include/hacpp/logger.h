#pragma once

#include <spdlog/spdlog.h>

#include <memory>
#include <string>
#include <string_view>

namespace hacpp::mqtt {

inline constexpr std::string_view Logger = "hacpp::mqtt";

namespace detail {

inline auto logger(std::string_view name = Logger) -> std::shared_ptr<spdlog::logger>
{
  if (auto log = spdlog::get(std::string{name})) {
    return log;
  }

  return spdlog::default_logger();
}

} // namespace detail

} // namespace hacpp::mqtt
