#pragma once

#include <hacpp/async_mqtt_client.h>
#include <hacpp/entity.h>
#include <hacpp/factory.h>
#include <hacpp/hacpp.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/json.hpp>

#include <functional>
#include <string>
#include <utility>

namespace hacpp::mqtt {

class Button : protected Entity<Button>
{
    using Base = Entity<Button>;
    friend Base;

  public:
    using Base::async_close;
    using Base::async_discovery;
    using Base::async_setup;
    using Base::async_subscribe;
    using Base::async_update_availability;
    using Base::executor;
    using Handler = std::function<boost::asio::awaitable<void>()>;

    struct Opt
    {
        static constexpr Property CommandTopic{"command_topic"};
        static constexpr Property PayloadPress{"payload_press"};
        static constexpr Property Device{"device"};
    };

    struct Defs
    {
        static constexpr auto Component = "button";
        static constexpr auto PayloadPress = "PRESS";
    };

    struct Config
    {
        std::string unique_id;
        QoS qos;
        EntityCfg cfg;
        Handler handler;
    };

    Button(Config config, ClientType client)
        : Base{std::move(client)}
        , config_(std::move(config))
    {}

    [[nodiscard]] const EntityCfg& config() const
    {
      return config_.cfg;
    }

  protected:
    boost::asio::awaitable<Error> async_discovery_impl()
    {
      auto json = config_.cfg.json();

      co_return co_await async_publish(
          default_component_discovery_topic(Defs::Component, config_.unique_id),
          json,
          config_.qos);
    }

    boost::asio::awaitable<Error> async_subscribe_impl()
    {
      auto sub_topics = std::vector<TopicSubopts>{
          {config_.cfg[Opt::CommandTopic], config_.qos}
      };

      co_return co_await async_subscribe(sub_topics);
    }

  public:
    boost::asio::awaitable<Error> async_run()
    {
      while (true) {
        auto res = co_await async_recv();
        if (!res) {
          co_return res.error();
        }

        res->visit([&](auto&& packet) {
          using PacketType = std::decay_t<decltype(packet)>;
          if constexpr (std::is_same_v<PacketType, async_mqtt::v5::publish_packet>) {
            if (packet.topic() == config_.cfg[Opt::CommandTopic] &&
                packet.payload() == config_.cfg[Opt::PayloadPress]) {
              if (config_.handler) {
                boost::asio::co_spawn(executor(), config_.handler(), boost::asio::detached);
              }
            }
          }
        });
      }

      co_return ErrorCode::Success;
    }

  private:
    Config config_;
};

template <>
class Factory<Button>
{
  public:
    Factory(std::string unique_id, ClientType client)
        : unique_id_(std::move(unique_id))
        , client_(std::move(client))
    {}

    template <typename T, typename V>
    auto& set(T&& key, V&& value)
    {
      cfg_.set(std::forward<T>(key), std::forward<V>(value));
      return *this;
    }

    auto& on_press(Button::Handler handler)
    {
      handler_ = std::move(handler);
      return *this;
    }

    auto create()
    {
      return Button{
          Button::Config{.unique_id = unique_id_, .qos = qos_, .cfg = cfg_, .handler = std::move(handler_)},
          std::move(client_)
      };
    }

  private:
    std::string unique_id_;
    EntityCfg cfg_{
        {Button::Opt::PayloadPress, Button::Defs::PayloadPress},
        {Button::Opt::CommandTopic, default_component_command_topic(Button::Defs::Component, unique_id_)}
    };
    QoS qos_ = QoS::at_least_once;
    ClientType client_;
    Button::Handler handler_;
};

} // namespace hacpp::mqtt
