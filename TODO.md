# TODO

## FIXME

- [x] Fix shared-client receive error handling.
  - `SharedAsyncMqttClient::async_recv()` broadcasts failed receive results to proxies, but then continues and dereferences the unexpected result as a `PublishPacket`.
  - Return immediately after broadcasting the error.
  - File: `lib/include/hacpp/shared_mqtt_client.h`.

- [x] Make `RecvResultQueue::pop_front()` robust against empty wakeups.
  - The queue waits once when empty, then unconditionally reads `queue_.front()`.
  - Timer cancellation can wake the coroutine without a queued result.
  - Use a loop or return a deterministic error if the queue is still empty.
  - File: `lib/include/hacpp/shared_mqtt_client.h`.

- [x] Cap reconnect backoff and avoid unsafe shift math.
  - `AsyncMqttClient2::async_handle_reconnect()` uses `1 << (attempt - 1)` for exponential backoff.
  - Keep delay bounded and use duration-safe arithmetic.
  - File: `lib/include/hacpp/async_mqtt_client.h`.

- [x] Clarify `async_wait_autoreconnect()` wakeup semantics.
  - The implementation ignores the wait error code, including `operation_aborted`.
  - If timer cancellation means "reconnect completed", encode that explicitly.
  - File: `lib/include/hacpp/async_mqtt_client.h`.

- [x] Define detached handler error semantics.
  - Button and Cover handlers are fire-and-forget side effects.
  - Handlers return `boost::asio::awaitable<void>`, so domain errors are intentionally not propagated through the entity API.
  - Files: `lib/include/hacpp/button.h`, `lib/include/hacpp/cover.h`.

- [x] Fix `hacpp.h` format include mismatch.
  - The header now uses `<format>` and `std::format`.
  - File: `lib/include/hacpp/hacpp.h`.

## High Priority

- [x] Keep entity `async_run()` loops alive after a successful reconnect.
  - `AsyncMqttClient2::async_recv()` now reports `ErrorCode::SessionLost` after reconnect.
  - `Entity::async_recv()` handles `SessionLost`, runs `async_setup()`, and continues receiving packets.
  - Files: `lib/include/hacpp/async_mqtt_client.h`, `lib/include/hacpp/entity.h`.

- [x] Restore entity subscriptions after reconnect.
  - `Entity::handle_err(ErrorCode::SessionLost)` calls `async_setup()`.
  - `async_setup()` re-runs discovery, subscribe, and availability update.
  - This restores command subscriptions for entities such as `button` and `cover`.
  - Files: `lib/include/hacpp/entity.h`, `lib/include/hacpp/button.h`, `lib/include/hacpp/cover.h`.

- [x] Decide raw client reconnect semantics.
  - No runtime behavior was changed.
  - Raw `AsyncMqttClient2::async_recv()` intentionally returns `ErrorCode::SessionLost` after a successful reconnect.
  - Entity wrappers treat that as recoverable and replay setup; direct client users need the same signal to restore session-level state such as subscriptions.
  - File: `lib/include/hacpp/async_mqtt_client.h`.

## Medium Priority

- [ ] Hide shared-client implementation details.
  - Move `RecvResultQueue` and `SharedClientState` under a `detail` namespace or otherwise keep them out of the public-facing API surface.
  - File: `lib/include/hacpp/shared_mqtt_client.h`.

- [x] Clarify the shared connection receive pump API.
  - `SharedAsyncMqttConnection::async_pump_one()` now receives one result from the real MQTT client and dispatches it to shared clients.
  - The one-shot pump contract is explicit while logical clients still use `SharedAsyncMqttClient::async_recv()`.
  - File: `lib/include/hacpp/shared_mqtt_client.h`.

- [x] Handle non-publish packets in shared receive dispatch.
  - `SharedAsyncMqttConnection::async_pump_one()` now logs a warning and skips dispatch for successful non-publish packets.
  - File: `lib/include/hacpp/shared_mqtt_client.h`.

- [x] Avoid duplicate shared-client registration.
  - `SharedAsyncMqttConnection::async_subscribe()` now refreshes existing topic entries and registers each live shared client state once.
  - File: `lib/include/hacpp/shared_mqtt_client.h`.

- [x] Remove expired shared clients during dispatch.
  - `SharedAsyncMqttConnection::async_pump_one()` now removes expired weak shared-client states before dispatch.
  - File: `lib/include/hacpp/shared_mqtt_client.h`.

- [ ] Clarify logical-client close semantics.
  - `SharedAsyncMqttClient::async_close()` unregisters the logical shared client, but does not close the MQTT connection.
  - Decide whether the current name is acceptable for entity interface compatibility or whether an internal detach/unregister helper should make the distinction explicit.
  - File: `lib/include/hacpp/shared_mqtt_client.h`.

- [x] Remove duplicated/unreachable error branch in `async_recv()`.
  - `AsyncMqttClient2::async_recv()` now has one transport-error branch that delegates reconnect handling, followed by the invalid-packet branch.
  - File: `lib/include/hacpp/async_mqtt_client.h`.

- [x] Make config reads non-mutating.
  - `EntityCfg::operator[]` now delegates to non-mutating `at()`.
  - Entity call sites still use `[]` where factory defaults or setup guarantee the key exists.
  - File: `lib/include/hacpp/entity.h`.

- [ ] Add capped reconnect backoff.
  - Current exponential backoff (`2^(attempt-1)`) can overflow `int` and produce excessive delays.
  - Target behavior: cap delay (e.g., max 30s/60s), use safe duration math.
  - File: `lib/include/hacpp/async_mqtt_client.h`.

## Low Priority

- [x] Implement `EntityCfg::set(const Device&)`.
  - Device metadata is serialized into the Home Assistant discovery `device` object.
  - File: `lib/include/hacpp/entity.h`.

- [ ] Provide sane default `state_topic` for `Cover` factory.
  - Currently commented out, requiring manual configuration for state updates.
  - Add default unless explicitly overridden.
  - File: `lib/include/hacpp/cover.h`.

## Suggested Validation After Fixes

- [x] Add client-level integration tests for reconnect scenarios.
  - Covered: autoreconnect signal, publish waits during autoreconnect, subscribe waits during autoreconnect.
  - File: `tests/integration_tests/client_tests.cpp`.
- [x] Add entity-level integration test for reconnect setup replay.
  - Covered: entity receive path observes reconnect, `Entity::async_recv()` reruns `async_setup()`, and discovery/subscribe are called again.
  - File: `tests/integration_tests/entity.cpp`.
- [ ] Extend entity-level reconnect command-flow tests.
  - Remaining: verify commands still trigger handlers post-reconnect.
- [ ] Add negative config tests for missing required keys to verify deterministic `InvalidConfig` behavior.
