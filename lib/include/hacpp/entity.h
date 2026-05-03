#pragma once

#include <boost/json.hpp>

namespace hacpp::mqtt {

    struct Property {
        std::string_view key;
        std::string_view obj_key;
    };


    struct Device
    {
        std::string configuraton_url;
        std::vector<std::string> connections;
        std::string hw_version;
        std::vector<std::string> identifiers;
        std::string manufacturer;
        std::string model;
        std::string model_id;
        std::string name;
        std::string serial_number;
        std::string suggested_area;
        std::string sw_version;
        std::string via_device;
    };


    struct Availability {
        // struct Opt {
        //     constexpr static Property Topic {"topic", "availability"};
        //     constexpr static Property PayloadAvailable {"payload_available", "availability"};
        //     constexpr static Property PayloadNotAvailable {"payload_not_available", "availability"};
        //     constexpr static Property ValueTemplate {"value_template", "availability"};
        // };

        struct Defs {
            constexpr static auto PayloadAvailable = "online";
            constexpr static auto PayloadNotAvailable = "offline";
        };

        std::string topic;
        std::string payload_available;
        std::string payload_not_available;
        std::string value_template;
    };


class EntityCfg
{
public:
    EntityCfg(std::initializer_list<std::pair<Property, std::string>> init)
    {
        for (auto [prop, value] : init) {
            set(prop, value);
        }
    }

    EntityCfg& set(const Property& prop, const std::string& value)
    {
        if (prop.obj_key.empty()) {
            obj_[prop.key] = value;
            return *this;
        }

        if (!obj_.contains(prop.obj_key)) {
            obj_[prop.obj_key] = boost::json::object{};
        }

        obj_[prop.obj_key].as_object()[prop.key] = value;

        return *this;
    }

    auto operator[](const Property& prop)
    {
        if (prop.obj_key.empty()) {
            return boost::json::value_to<std::string>(obj_[prop.key]);
        }

        return boost::json::value_to<std::string>(obj_[prop.obj_key].as_object()[prop.key]);
    }

    auto at(const Property& prop) const
    {
        if (prop.obj_key.empty()) {
            return boost::json::value_to<std::string>(obj_.at(prop.key));
        }

        return boost::json::value_to<std::string>(obj_.at(prop.obj_key).as_object().at(prop.key));
    }

    void set(const Device& device)
    {
        // obj_["device"] = boost::json::serialize(device);
    }

    void set(const Availability& availability)
    {
        // obj_["availability"] = boost::json::serialize(availability);
    }

    auto json() const
    {
        return boost::json::serialize(obj_);
    }

private:
    boost::json::object obj_;
};
}
