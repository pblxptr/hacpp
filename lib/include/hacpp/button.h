#pragma once

#include <hacpp/factory.h>
#include <hacpp/async_mqtt_client.h>
#include <hacpp/hacpp.h>
#include <hacpp/entity.h>

#include <string>
#include <functional>
#include <boost/json.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>

namespace hacpp::mqtt {

    class Button
    {
    public:
        using Handler = std::function<boost::asio::awaitable<void>()>;

        struct Opt {
            constexpr static Property CommandTopic {"command_topic"};
            constexpr static Property PayloadPress {"payload_press"};
            constexpr static Property Device {"device"};
        };

        struct Defs {
            constexpr static auto Component = "button";
            constexpr static auto PayloadPress = "PRESS";
        };

        struct Config {
            std::string unique_id;
            QoS qos;
            EntityCfg cfg;
            Handler handler;
        };

        Button(Config config, ClientType client)
            : config_(std::move(config))
            , client_(std::move(client))
        {
        }

        boost::asio::awaitable<Error> async_discovery()
        {
            auto json = config_.cfg.json();
            auto err = co_await client_.async_subscribe({
                { config_.cfg[Opt::CommandTopic], config_.qos }
            });

            if (err) {
                co_return err;
            }

            co_return co_await client_.async_publish(
                component_discovery_topic(Defs::Component, config_.unique_id),
                json,
                config_.qos
            );
        }

        boost::asio::awaitable<Error> async_run()
        {
            while (true) {
                auto res = co_await client_.async_recv();
                if (!res) {
                    co_return res.error();
                }

                res->visit([&](auto&& packet) {
                    using PacketType = std::decay_t<decltype(packet)>;
                    if constexpr (std::is_same_v<PacketType, async_mqtt::v5::publish_packet>) {
                        if (packet.topic() == config_.cfg[Opt::CommandTopic] &&
                            packet.payload() == config_.cfg[Opt::PayloadPress]) {
                            if (config_.handler) {
                                boost::asio::co_spawn(client_.executor(), config_.handler(), boost::asio::detached);
                            }
                        }
                    }
                });
            }

            co_return ErrorCode::Success;
        }

    private:
        Config config_;
        ClientType client_;
    };

    template <>
    class Factory<Button>
    {
    public:
        Factory(const std::string& unique_id, ClientType client)
            : unique_id_(unique_id),
              client_(std::move(client))
        {
        }

        auto& on_press(Button::Handler handler)
        {
            handler_ = std::move(handler);
            return *this;
        }

        auto create()
        {
            return Button{
                Button::Config {
                    .unique_id = unique_id_,
                    .qos = qos_,
                    .cfg = cfg_,
                    .handler = std::move(handler_)
                },
                std::move(client_)
            };
        }

    private:
        std::string unique_id_;
        EntityCfg cfg_ {
            { Button::Opt::PayloadPress, Button::Defs::PayloadPress },
            { Button::Opt::CommandTopic, default_component_command_topic(Button::Defs::Component, unique_id_) }
        };
        QoS qos_ = QoS::at_least_once;
        ClientType client_;
        Button::Handler handler_;
    };

} // namespace hacpp::mqtt
