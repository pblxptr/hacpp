#pragma once

#include <hacpp/async_mqtt_client.h>

#include <boost/json.hpp>

#include <initializer_list>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace hacpp::mqtt {

struct Property
{
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

struct Availability
{
  struct Opt
  {
    static constexpr Property Topic{"topic", "availability"};
    static constexpr Property PayloadAvailable{"payload_available", "availability"};
    static constexpr Property PayloadNotAvailable{"payload_not_available", "availability"};
    static constexpr Property ValueTemplate{"value_template", "availability"};
  };

  struct Defs
  {
    static constexpr auto PayloadAvailable = "online";
    static constexpr auto PayloadNotAvailable = "offline";
  };
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

  bool contains(const Property& prop) const
  {
    if (prop.obj_key.empty()) {
      return obj_.contains(prop.key);
    }

    return obj_.contains(prop.obj_key) && obj_.at(prop.obj_key).as_object().contains(prop.key);
  }

  auto json() const
  {
    return boost::json::serialize(obj_);
  }

  private:
  boost::json::object obj_;
};

template <typename Impl>
class Entity
{
  public:
  explicit Entity(ClientType client)
      : client_{std::move(client)}
  {}

  auto executor()
  {
    return client_.executor();
  }

  boost::asio::awaitable<Error> async_setup()
  {
    auto err = co_await async_discovery();
    if (err) {
      co_return err;
    }

    err = co_await async_subscribe();
    if (err) {
      co_return err;
    }

    co_return co_await async_update_availability(true);
  }

  boost::asio::awaitable<Error> async_discovery()
  {
    co_return co_await impl().async_discovery_impl();
  }

  boost::asio::awaitable<Error> async_subscribe()
  {
    co_return co_await impl().async_subscribe_impl();
  }

  boost::asio::awaitable<Error> async_update_availability(bool state)
  {
    co_return co_await impl().async_update_availability_impl(state);
  }

  template <typename... Args>
  boost::asio::awaitable<Error> async_publish(Args&&... args)
  {
    co_return co_await client_.async_publish(std::forward<Args>(args)...);
  }

  boost::asio::awaitable<Error> async_subscribe(const std::vector<TopicSubopts>& sub_entry)
  {
    co_return co_await client_.async_subscribe(sub_entry);
  }

  boost::asio::awaitable<RecvResult> async_recv()
  {
    while (true) {
      auto packet = co_await client_.async_recv();
      if (packet) {
        co_return packet;
      }

      auto err = co_await handle_err(packet.error());
      if (err) {
        co_return std::unexpected{err};
      }
    }
  }

  boost::asio::awaitable<void> async_close()
  {
    co_await client_.async_close();
  }

  private:
  Impl& impl()
  {
    return static_cast<Impl&>(*this);
  }

  boost::asio::awaitable<Error> handle_err(const Error& err)
  {
    if (err != ErrorCode::SessionLost) {
      co_return err;
    }

    co_return co_await async_setup();
  }

  private:
  ClientType client_;
};

} // namespace hacpp::mqtt
