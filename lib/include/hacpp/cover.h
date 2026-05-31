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

namespace hacpp::mqtt {

class Cover : protected Entity<Cover>
{
  using Base = Entity<Cover>;
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

  Cover(Config config, ClientType client)
      : Base{std::move(client)}
      , config_(std::move(config))
  {}

  const EntityCfg& config() const
  {
    return config_.cfg;
  }

  boost::asio::awaitable<Error> async_update_state(const std::string& state)
  {
    if (!config_.cfg.contains(Opt::StateTopic)) {
      co_return ErrorCode::InvalidConfig;
    }

    co_return co_await async_publish(config_.cfg[Opt::StateTopic], state, config_.qos);
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
          if (packet.topic() == config_.cfg[Opt::CommandTopic]) {
            auto payload = packet.payload();
            if (payload == config_.cfg[Opt::PayloadOpen]) {
              if (config_.on_open) {
                boost::asio::co_spawn(executor(), config_.on_open(), boost::asio::detached);
              }
            } else if (payload == config_.cfg[Opt::PayloadClose]) {
              if (config_.on_close) {
                boost::asio::co_spawn(executor(), config_.on_close(), boost::asio::detached);
              }
            } else if (payload == config_.cfg[Opt::PayloadStop]) {
              if (config_.on_stop) {
                boost::asio::co_spawn(executor(), config_.on_stop(), boost::asio::detached);
              }
            }
          }
        }
      });
    }

    co_return ErrorCode::Success;
  }

  protected:
  boost::asio::awaitable<Error> async_update_availability_impl(bool state)
  {
    /*
      TODO:
        - Move the implementation to Entity
        - Some of entities do not require awailability so allow to succeed when it is not mandatory
        - Bear in mind the Success is returned here, this is valid for Cover, not for the rest of entities
    */
    if (!config_.cfg.contains(Availability::Opt::Topic)) {
      co_return ErrorCode::Success;
    }

    auto val = std::string{};
    if (state) {
      val = config_.cfg.contains(Availability::Opt::PayloadAvailable) ? config_.cfg[Availability::Opt::PayloadAvailable]
                                                                      : Availability::Defs::PayloadAvailable;
    } else {
      val = config_.cfg.contains(Availability::Opt::PayloadNotAvailable)
              ? config_.cfg[Availability::Opt::PayloadNotAvailable]
              : Availability::Defs::PayloadNotAvailable;
    }

    co_return co_await async_publish(config_.cfg[Availability::Opt::Topic], val, config_.qos);
  }

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

  private:
  Config config_;
};

template <>
class Factory<Cover>
{
  public:
  Factory(const std::string& unique_id, ClientType client)
      : unique_id_(unique_id)
      , client_(std::move(client))
  {}

  template <typename T, typename V>
  auto& set(T&& key, V&& value)
  {
    cfg_.set(std::forward<T>(key), std::forward<V>(value));
    return *this;
  }

  auto& on_open(Cover::Handler handler)
  {
    on_open_ = std::move(handler);
    if (!cfg_.contains(Cover::Opt::PayloadOpen)) {
      cfg_.set(Cover::Opt::PayloadOpen, Cover::Defs::PayloadOpen);
    }

    return *this;
  }

  auto& on_close(Cover::Handler handler)
  {
    on_close_ = std::move(handler);
    if (!cfg_.contains(Cover::Opt::PayloadClose)) {
      cfg_.set(Cover::Opt::PayloadClose, Cover::Defs::PayloadClose);
    }

    return *this;
  }

  auto& on_stop(Cover::Handler handler)
  {
    on_stop_ = std::move(handler);
    if (!cfg_.contains(Cover::Opt::PayloadStop)) {
      cfg_.set(Cover::Opt::PayloadStop, Cover::Defs::PayloadStop);
    }

    return *this;
  }

  auto create()
  {
    return Cover{
        Cover::Config{
                      .unique_id = unique_id_,
                      .qos = qos_,
                      .cfg = cfg_,
                      .on_open = std::move(on_open_),
                      .on_close = std::move(on_close_),
                      .on_stop = std::move(on_stop_)},
        std::move(client_)
    };
  }

  private:
  std::string unique_id_;
  EntityCfg cfg_{
      {Cover::Opt::CommandTopic, default_component_command_topic(Cover::Defs::Component, unique_id_)},
      // { Cover::Opt::StateTopic,
      // default_component_state_topic(Cover::Defs::Component, unique_id_) }
  };
  QoS qos_ = QoS::at_least_once;
  ClientType client_;
  Cover::Handler on_open_;
  Cover::Handler on_close_;
  Cover::Handler on_stop_;
};

} // namespace hacpp::mqtt
