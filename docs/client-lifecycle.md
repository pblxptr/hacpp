# Client lifecycle

hacpp is built on Boost.Asio coroutines. Application code owns the top-level lifecycle:

1. Create a client.
2. Connect it.
3. Create entities from that client.
4. Call `async_setup()` on each entity.
5. Keep the receive path running.
6. Close entities or clients during shutdown.

## Unique client per entity

Use a unique `AsyncMqttClient` when an entity should own its own MQTT connection.

```cpp
boost::asio::awaitable<void> run_sensor(boost::asio::any_io_executor exe)
{
  auto client = hacpp::mqtt::AsyncMqttClient{exe, config};
  auto err = co_await client.async_connect();
  if (err) {
    co_return;
  }

  auto sensor = hacpp::mqtt::factory<hacpp::mqtt::Sensor>("temperature", std::move(client))
                    .create();

  err = co_await sensor.async_setup();
  if (err) {
    co_return;
  }

  boost::asio::co_spawn(sensor.executor(), sensor.async_run(), boost::asio::detached);
}
```

Only one coroutine should call `async_recv()` or `async_run()` for a unique client at a time.

## Shared connection

Use `SharedAsyncMqttConnection` when many entities should share one physical MQTT connection.

```cpp
boost::asio::awaitable<void> run_shared(boost::asio::any_io_executor exe)
{
  auto connection = hacpp::mqtt::SharedAsyncMqttConnection::create(
      hacpp::mqtt::AsyncMqttClient{exe, config});

  auto err = co_await connection->async_connect();
  if (err) {
    co_return;
  }

  boost::asio::co_spawn(connection->executor(), connection->async_run(), boost::asio::detached);

  auto button = hacpp::mqtt::factory<hacpp::mqtt::Button>(
                    "restart", connection->make_client())
                    .on_press([]() -> boost::asio::awaitable<void> {
                      co_return;
                    })
                    .create();

  err = co_await button.async_setup();
  if (!err) {
    boost::asio::co_spawn(button.executor(), button.async_run(), boost::asio::detached);
  }
}
```

`SharedAsyncMqttConnection::async_run()` is the single receive owner for the physical client. Logical shared clients receive packets from per-client queues.

Do not run more than one `async_run()` loop for the same shared connection.

## Setup and run

`async_setup()` publishes discovery, subscribes command topics when the entity has them, and publishes availability when configured.

`async_run()` keeps the entity receive path alive. For command entities it also dispatches incoming commands. For state-only entities it is still useful because receive-driven reconnect needs an active receive path.
