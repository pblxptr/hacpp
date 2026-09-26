# Errors and reconnect

hacpp reports errors as `boost::system::error_code` values using the `hacpp::mqtt` error category.

Most public async APIs return `boost::asio::awaitable<Error>`.

Receive APIs return:

```cpp
using RecvResult = std::expected<async_mqtt::packet_variant, Error>;
```

## Receive-driven reconnect

Reconnect is driven by the receive path. A broken TCP/MQTT connection is normally discovered when the client reads from or writes to the socket. hacpp centralizes automatic reconnect in `AsyncMqttClient::async_recv()`.

That means applications should keep one receive loop active:

- unique client: one entity `async_run()` or one caller of `async_recv()`;
- shared connection: one `SharedAsyncMqttConnection::async_run()`.

Publish and subscribe wait if the client is already reconnecting. They do not independently start a second reconnect path.

## SessionReset

`ErrorCode::SessionReset` means reconnect succeeded, but MQTT session state should be restored.

Entities handle this automatically inside `Entity::async_recv()` by running setup again. This republishes discovery, recreates subscriptions, and updates availability.

If you use `AsyncMqttClient` directly, treat `SessionReset` as a signal to resubscribe and restore application state.

## Shared-client forwarding

`SharedAsyncMqttConnection::async_run()` reads from the physical client and forwards publish packets to logical shared clients based on subscribed topics.

When the physical client reports an error, the shared connection forwards that error to all registered logical clients. `SessionReset` is forwarded so each entity can replay setup through its normal receive loop.

## Shutdown

Entity `async_close()` forwards to the underlying client and returns an `Error`.

For shared clients, `SharedAsyncMqttClient::async_close()` detaches that logical client from the shared connection. Closing the physical MQTT connection is done through `SharedAsyncMqttConnection::async_close()`.
