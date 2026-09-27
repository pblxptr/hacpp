#pragma once

#include <spdlog/spdlog.h>

#include <memory>
#include <string>
#include <string_view>

namespace hacpp::mqtt {

inline constexpr std::string_view Logger = "hacpp::mqtt";

namespace detail {

inline auto default_named_logger(std::string_view name) -> std::shared_ptr<spdlog::logger>
{
  const auto default_logger = spdlog::default_logger();
  auto logger = std::make_shared<spdlog::logger>(
      std::string{name},
      default_logger->sinks().begin(),
      default_logger->sinks().end());

  logger->set_level(default_logger->level());
  logger->flush_on(default_logger->flush_level());

  return logger;
}

inline auto fallback_logger() -> std::shared_ptr<spdlog::logger>
{
  static auto logger = default_named_logger(Logger);
  return logger;
}

inline auto logger() -> std::shared_ptr<spdlog::logger>
{
  if (auto log = spdlog::get(std::string{Logger})) {
    return log;
  }

  return fallback_logger();
}

} // namespace detail

} // namespace hacpp::mqtt
