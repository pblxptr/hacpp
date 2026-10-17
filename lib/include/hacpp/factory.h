#pragma once

#include <hacpp/async_mqtt_client.h>

#include <string>
#include <utility>

namespace hacpp::mqtt {
template <template <typename> typename Entity, typename Client = ClientType>
class Factory;

class CommonFactory
{
public:
    template <typename Key>
    auto& set(EntityCfg& cfg, Key&& key, nullptr_t)
    {
      cfg.set(std::forward<Key>(key), nullptr);
      return *this;
    }

    auto& set(EntityCfg& cfg, const Device& device)
    {
      cfg.set(device);
      return *this;
    }

    template <typename T, typename V>
    auto& set(EntityCfg& cfg, T&& key, V&& value)
    {
      cfg.set(std::forward<T>(key), std::forward<V>(value));
      return *this;
    }
};

template <template <typename> typename Entity, typename Client>
auto factory(std::string id, Client client)
{
  return Factory<Entity, Client>{std::move(id), std::move(client)};
}

} // namespace hacpp::mqtt
