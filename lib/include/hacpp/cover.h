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

struct CoverCfg
{
    using Handler = std::function<boost::asio::awaitable<void>()>;

    struct Opt
    {
        static constexpr Property CommandTopic{"command_topic"};
        static constexpr Property StateTopic{"state_topic"};
        static constexpr Property PayloadOpen{"payload_open"};
        static constexpr Property PayloadClose{"payload_close"};
        static constexpr Property PayloadStop{"payload_stop"};
        static constexpr Property StateOpen{"state_open"};
        static constexpr Property StateClosed{"state_closed"};
        static constexpr Property StateOpening{"state_opening"};
        static constexpr Property StateClosing{"state_closing"};
        static constexpr Property Device{"device"};
    };

    struct Defs
    {
        static constexpr auto Component = "cover";
        static constexpr auto PayloadOpen = "OPEN";
        static constexpr auto PayloadClose = "CLOSE";
        static constexpr auto PayloadStop = "STOP";
        static constexpr auto StateOpen = "open";
        static constexpr auto StateClosed = "closed";
        static constexpr auto StateOpening = "opening";
        static constexpr auto StateClosing = "closing";
    };

    struct Config
    {
        std::string unique_id;
        QoS qos;
        EntityCfg cfg;
        Handler on_open;
        Handler on_close;
        Handler on_stop;
    };
};

template <typename Client = ClientType>
class Cover : protected Entity<Cover<Client>, Client>
{
    using Base = Entity<Cover<Client>, Client>;
    using Base::async_publish;
    using Base::async_recv;
    friend Base;

  public:
    using Base::async_close;
    using Base::async_discovery;
    using Base::async_setup;
    using Base::async_subscribe;
    using Base::async_update_availability;
    using Base::executor;
    Cover(CoverCfg::Config config, Client client)
        : Base{std::move(client)}
        , config_(std::move(config))
    {}

    [[nodiscard]] const EntityCfg& config() const
    {
      return config_.cfg;
    }

    boost::asio::awaitable<Error> async_update_state(std::string state)
    {
      if (!config_.cfg.contains(CoverCfg::Opt::StateTopic)) {
        co_return ErrorCode::InvalidConfig;
      }

      co_return co_await async_publish(config_.cfg[CoverCfg::Opt::StateTopic], state, config_.qos);
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
            if (packet.topic() == config_.cfg[CoverCfg::Opt::CommandTopic]) {
              auto payload = packet.payload();
              auto cmd = std::string_view{payload.data(), payload.size()};
              dispatch(cmd);
            }
          }
        });
      }

      co_return ErrorCode::Success;
    }

  protected:
    void dispatch(std::string_view cmd)
    {
      if (cmd == config_.cfg[CoverCfg::Opt::PayloadOpen]) {
        if (config_.on_open) {
          boost::asio::co_spawn(this->executor(), config_.on_open(), boost::asio::detached);
        }
      } else if (cmd == config_.cfg[CoverCfg::Opt::PayloadClose]) {
        if (config_.on_close) {
          boost::asio::co_spawn(this->executor(), config_.on_close(), boost::asio::detached);
        }
      } else if (cmd == config_.cfg[CoverCfg::Opt::PayloadStop]) {
        if (config_.on_stop) {
          boost::asio::co_spawn(this->executor(), config_.on_stop(), boost::asio::detached);
        }
      }
    }

    boost::asio::awaitable<Error> async_discovery_impl()
    {
      auto json = config_.cfg.json();

      co_return co_await async_publish(
          default_component_discovery_topic(CoverCfg::Defs::Component, config_.unique_id),
          json,
          config_.qos);
    }

    boost::asio::awaitable<Error> async_subscribe_impl()
    {
      auto sub_topics = std::vector<TopicSubopts>{
          {config_.cfg[CoverCfg::Opt::CommandTopic], config_.qos}
      };

      co_return co_await async_subscribe(sub_topics);
    }

  private:
    CoverCfg::Config config_;
};

template <typename Client>
class Factory<Cover, Client>
{
  public:
    Factory(std::string unique_id, Client client)
        : unique_id_(std::move(unique_id))
        , client_(std::move(client))
    {}

    template <typename T, typename V>
    auto& set(T&& key, V&& value)
    {
      cfg_.set(std::forward<T>(key), std::forward<V>(value));
      return *this;
    }

    auto& on_open(CoverCfg::Handler handler)
    {
      on_open_ = std::move(handler);
      if (!cfg_.contains(CoverCfg::Opt::PayloadOpen)) {
        cfg_.set(CoverCfg::Opt::PayloadOpen, CoverCfg::Defs::PayloadOpen);
      }

      return *this;
    }

    auto& on_close(CoverCfg::Handler handler)
    {
      on_close_ = std::move(handler);
      if (!cfg_.contains(CoverCfg::Opt::PayloadClose)) {
        cfg_.set(CoverCfg::Opt::PayloadClose, CoverCfg::Defs::PayloadClose);
      }

      return *this;
    }

    auto& on_stop(CoverCfg::Handler handler)
    {
      on_stop_ = std::move(handler);
      if (!cfg_.contains(CoverCfg::Opt::PayloadStop)) {
        cfg_.set(CoverCfg::Opt::PayloadStop, CoverCfg::Defs::PayloadStop);
      }

      return *this;
    }

    auto create()
    {
      // clang-format off
      return Cover<Client>{
          CoverCfg::Config{
            .unique_id = unique_id_,
            .qos = qos_,
            .cfg = cfg_,
            .on_open = std::move(on_open_),
            .on_close = std::move(on_close_),
            .on_stop = std::move(on_stop_)
          },
          std::move(client_)
      };
      // clang-format on
    }

  private:
    std::string unique_id_;
    EntityCfg cfg_{
        {CoverCfg::Opt::CommandTopic, default_component_command_topic(CoverCfg::Defs::Component, unique_id_)},
        // { CoverCfg::Opt::StateTopic,
        // default_component_state_topic(CoverCfg::Defs::Component, unique_id_) }
    };
    QoS qos_ = QoS::at_least_once;
    Client client_;
    CoverCfg::Handler on_open_;
    CoverCfg::Handler on_close_;
    CoverCfg::Handler on_stop_;
};

} // namespace hacpp::mqtt
