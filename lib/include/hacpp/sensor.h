#pragma once

#include <hacpp/async_mqtt_client.h>
#include <hacpp/entity.h>
#include <hacpp/factory.h>
#include <hacpp/hacpp.h>

#include <boost/json.hpp>

#include <string>
#include <utility>

namespace hacpp::mqtt {

class Sensor : protected Entity<Sensor>
{
  using Base = Entity<Sensor>;
  friend Base;

  public:
  using Base::async_close;
  using Base::async_discovery;
  using Base::async_setup;
  using Base::async_subscribe;
  using Base::async_update_availability;
  using Base::executor;
  struct Opt
  {
    static constexpr Property StateTopic{"state_topic"};
    static constexpr Property Device{"device"};
  };

  struct Defs
  {
    static constexpr auto Component = "sensor";
    static constexpr auto StateTopic = "state";
  };

  struct Config
  {
    std::string unique_id;
    QoS qos;
    EntityCfg cfg;
  };

  Sensor(Config config, ClientType client)
      : Base{std::move(client)}
      , config_(std::move(config))
  {}

  [[nodiscard]] const EntityCfg& config() const
  {
    return config_.cfg;
  }

  boost::asio::awaitable<Error> async_update_state(std::string state)
  {
    co_return co_await async_publish(config_.cfg[Opt::StateTopic], std::move(state), config_.qos);
  }

  protected:
  boost::asio::awaitable<Error> async_update_availability_impl(bool state)
  {
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
    co_return Error{};
  }

  public:
  boost::asio::awaitable<Error> async_run()
  {
    while (true) {
      auto recv_result = co_await async_recv();

      if (!recv_result) {
        co_return recv_result.error();
      }
    }
  }

  private:
  Config config_;
};

template <>
class Factory<Sensor>
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

  auto create()
  {
    return Sensor{
        Sensor::Config{.unique_id = unique_id_, .qos = qos_, .cfg = cfg_},
        std::move(client_)
    };
  }

  private:
  std::string unique_id_;
  EntityCfg cfg_{
      {Sensor::Opt::StateTopic, default_component_state_topic(Sensor::Defs::Component, unique_id_)}
  };
  QoS qos_ = QoS::at_most_once;
  ClientType client_;
};
} // namespace hacpp::mqtt
