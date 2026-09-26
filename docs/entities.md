# Entities

Entities wrap Home Assistant MQTT discovery and the common publish/subscribe patterns.

Supported entity types:

- `Sensor`: publishes string/numeric state.
- `BinarySensor`: publishes on/off state.
- `Button`: subscribes to a command topic and runs an `on_press` handler.
- `Cover`: subscribes to a command topic and runs open/close/stop handlers.

## Creating an entity

Factories provide defaults for discovery, state, and command topics.

```cpp
auto sensor = hacpp::mqtt::factory<hacpp::mqtt::Sensor>("temperature", std::move(client))
                  .set(hacpp::mqtt::SensorCfg::Opt::StateTopic, "home/temperature/state")
                  .create();
```

The `unique_id` passed to `factory()` is used in the default Home Assistant discovery topic and default MQTT topics.

## Setup

Call `async_setup()` after the underlying client is connected.

```cpp
auto err = co_await sensor.async_setup();
if (err) {
  co_return;
}
```

Setup performs the entity-specific discovery publish, command subscriptions if needed, and availability publish if an availability topic is configured.

## Publishing state

State entities expose `async_update_state()`.

```cpp
co_await sensor.async_update_state("21.5");
co_await binary_sensor.async_update_state(true);
co_await cover.async_update_state("open");
```

`Cover::async_update_state()` returns `InvalidConfig` if no state topic is configured.

## Handling commands

Command entities run user handlers from `async_run()`.

```cpp
auto button = hacpp::mqtt::factory<hacpp::mqtt::Button>("restart", std::move(client))
                  .on_press([]() -> boost::asio::awaitable<void> {
                    // Do application work here.
                    co_return;
                  })
                  .create();
```

For covers:

```cpp
auto cover = hacpp::mqtt::factory<hacpp::mqtt::Cover>("garage", std::move(client))
                 .on_open([]() -> boost::asio::awaitable<void> { co_return; })
                 .on_close([]() -> boost::asio::awaitable<void> { co_return; })
                 .on_stop([]() -> boost::asio::awaitable<void> { co_return; })
                 .create();
```

Handlers are spawned on the entity executor. Keep handlers short or move long work into application-owned tasks.

## Configuration values

Use `Factory::set()` with the entity's `Cfg::Opt` keys to override default MQTT discovery fields.

```cpp
factory.set(hacpp::mqtt::Availability::Opt::Topic, "home/device/availability");
```

Required fields are read with `EntityCfg::at()`, so missing required configuration is treated as a programming/configuration error.
