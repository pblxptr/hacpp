#pragma once

#include <hacpp/factory.h>
#include <hacpp/async_mqtt_client.h>
#include <hacpp/hacpp.h>
#include <hacpp/entity.h>

#include <string>
#include <boost/json.hpp>

namespace hacpp::mqtt {

    class BinarySensor
    {
    public:
        struct Opt {
            constexpr static Property PayloadOn {"payload_on"};
            constexpr static Property PayloadOff {"payload_off"};
            constexpr static Property StateTopic {"state_topic"};
            constexpr static Property Device {"device"};
        };

        struct Defs {
            constexpr static auto Component = "binary_sensor";
            constexpr static auto PayloadOn = "ON";
            constexpr static auto PayloadOff = "OFF";
            constexpr static auto StateTopic = "state";
        };

        struct Config {
            std::string unique_id;
            QoS qos;
            EntityCfg cfg;
        };

        BinarySensor(Config config, ClientType client)
            : config_(std::move(config))
            , client_(std::move(client))
        {
        }

        const auto& config() const
        {
            return config_;
        }

        boost::asio::awaitable<Error> async_update_state(bool state)
        {
            co_return co_await client_.async_publish(config_.cfg[Opt::StateTopic],
                state
                    ? config_.cfg[Opt::PayloadOn]
                    : config_.cfg[Opt::PayloadOff]
                , config_.qos);
        }

        boost::asio::awaitable<Error> async_discovery()
        {
            auto json = config_.cfg.json();

            co_return co_await client_.async_publish(
                component_discovery_topic(Defs::Component, config_.unique_id),
                json,
                config_.qos
            );
        }

        boost::asio::awaitable<Error> async_run()
        {
            while (true) {
                auto recv_result = co_await client_.async_recv();

                if (!recv_result) {
                    co_return recv_result.error();
                }
            }
        }

        boost::asio::awaitable<void> async_close()
        {
            co_await client_.async_close();
        }

    private:
        Config config_;
        ClientType client_;
    };

    template <>
    class Factory<BinarySensor>
    {
    public:
        Factory(const std::string& unique_id, ClientType client)
            : unique_id_(unique_id),
              client_(std::move(client))
        {
        }

        template <typename T, typename V>
        auto& set(T&& key, V&& value)
        {
            cfg_.set(std::forward<T>(key), std::forward<V>(value));
            return *this;
        }

        auto& qos(QoS qos)
        {
            qos_ = qos;
            return *this;
        }

        auto create()
        {
            return BinarySensor{
                BinarySensor::Config{
                    .unique_id = unique_id_,
                    .qos = qos_,
                    .cfg = cfg_
                    },
                    std::move(client_)
                };
        }
    private:
        std::string unique_id_;
        EntityCfg cfg_ {
            {BinarySensor::Opt::PayloadOn, BinarySensor::Defs::PayloadOn},
            {BinarySensor::Opt::PayloadOff, BinarySensor::Defs::PayloadOff},
            {BinarySensor::Opt::StateTopic, default_component_state_topic(BinarySensor::Defs::Component, unique_id_)}
        };
        QoS qos_ = QoS::at_most_once;
        ClientType client_;
    };
} // namespace hacpp::mqtt
