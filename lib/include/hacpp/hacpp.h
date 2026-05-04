#pragma once

#include <unordered_map>
#include <string_view>
#include <string>
#include <format>

namespace hacpp::mqtt {
    constexpr static auto HomeAssistantPrefix = "homeassistant";

    inline auto default_component_discovery_topic(std::string_view component, std::string_view unique_id)
    {
        return fmt::format("{}/{}/{}/config", HomeAssistantPrefix, component, unique_id);
    }

    inline auto default_component_state_topic(std::string_view component, std::string_view unique_id)
    {
        return fmt::format("{}/{}/{}/state", HomeAssistantPrefix, component, unique_id);
    }

    inline auto default_component_command_topic(std::string_view component, std::string_view unique_id)
    {
        return fmt::format("{}/{}/{}/set", HomeAssistantPrefix, component, unique_id);
    }

    inline auto default_component_availability_topic(std::string_view component, std::string_view unique_id)
    {
        return fmt::format("{}/{}/{}/availability", HomeAssistantPrefix, component, unique_id);
    }
};
