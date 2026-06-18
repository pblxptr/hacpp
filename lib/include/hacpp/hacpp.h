#pragma once

#include <format>
#include <string>
#include <string_view>
#include <unordered_map>

namespace hacpp::mqtt {
static constexpr auto HomeAssistantPrefix = "homeassistant";

inline auto default_component_discovery_topic(std::string_view component, std::string_view unique_id)
{
  return std::format("{}/{}/{}/config", HomeAssistantPrefix, component, unique_id);
}

inline auto default_component_state_topic(std::string_view component, std::string_view unique_id)
{
  return std::format("{}/{}/{}/state", HomeAssistantPrefix, component, unique_id);
}

inline auto default_component_command_topic(std::string_view component, std::string_view unique_id)
{
  return std::format("{}/{}/{}/set", HomeAssistantPrefix, component, unique_id);
}

inline auto default_component_availability_topic(std::string_view component, std::string_view unique_id)
{
  return std::format("{}/{}/{}/availability", HomeAssistantPrefix, component, unique_id);
}
}; // namespace hacpp::mqtt
