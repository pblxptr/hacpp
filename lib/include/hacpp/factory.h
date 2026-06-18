#pragma once

#include <hacpp/async_mqtt_client.h>

#include <string>
#include <utility>

namespace hacpp::mqtt {
template <template <typename> typename Entity, typename Client = ClientType>
class Factory;

template <template <typename> typename Entity, typename Client>
auto factory(std::string id, Client client)
{
  return Factory<Entity, Client>{std::move(id), std::move(client)};
}

} // namespace hacpp::mqtt
