# Home Assistant C++ (hacpp) MQTT Entities Architecture Plan

## Objective
Develop a resilient, thread-safe C++ library on top of `async_mqtt` and `Boost.Asio` to manage Home Assistant entities (Binary Sensor, Sensor, Button) via MQTT. The library will provide a minimal asynchronous API supporting C++20 coroutines and Asio completion tokens, automatically handle connection drops, and safely integrate with external threads.

## Architecture & State Management

### The Connection Wrapper
To ensure resilience and simplify entity logic, a connection wrapper (e.g., `ResilientMqttClient`) will be created. 
- It wraps `async_mqtt::client`.
- It is strictly bound to a `boost::asio::strand` to ensure all internal state mutations are sequential, eliminating the need for `std::mutex`.
- **State Machine:**
  - `DISCONNECTED`: Initial state or terminal failure.
  - `CONNECTING`: Attempting to establish TCP/MQTT connection.
  - `CONNECTED`: Fully operational.
  - `RECONNECTING`: Connection dropped; attempting backoff reconnection.

### Buffer & Reconnect Logic
- If the connection drops, the wrapper transitions to `RECONNECTING` and starts a backoff timer.
- Incoming `async_publish` requests during the `RECONNECTING` phase are placed into a `std::deque<PendingPublish>`.
- The caller's asynchronous operation (coroutine) remains suspended.
- Upon successful reconnection and configuration (via `async_config`), the wrapper automatically drains the queue, publishes the pending payloads, and finally completes the suspended user operations.

### Entity Types
1. **Binary Sensor:** Read-only (from HA perspective). Uses `async_publish` for state updates.
2. **Sensor:** Read-only. Uses `async_publish` for state updates.
3. **Button:** Read-write. Uses `async_publish` for state updates and requires an active `async_recv` loop to handle incoming commands from the MQTT broker.

*Future-proofing:* Initially, each entity will instantiate its own `ResilientMqttClient`. The design will allow injecting a shared `ResilientMqttClient` instance in the future.

## Proposed API

### 1. Initialization
Entities are constructed with an executor (typically a strand) to ensure thread-safety.
```cpp
// Example construction
boost::asio::io_context ioc;
auto strand = boost::asio::make_strand(ioc);

hacpp::BinarySensor sensor(strand, /* HA config params */);
```

### 2. The Main Loop (`async_run`)
The primary lifecycle method. It handles connection, configuration, and receiving commands. It runs indefinitely until cancelled or a fatal error occurs.

```cpp
// Returns awaitable, throws system_error on fatal failure
boost::asio::awaitable<void> async_run(); 

// Using with completion token (e.g., catching error code)
template<typename CompletionToken>
auto async_run(CompletionToken&& token);
```
*Fatal Failure Handling:* If reconnection exhausts all retries, `async_run` completes with an error (e.g., throwing `boost::system::system_error` in a coroutine, or passing `boost::system::error_code` to the completion token).

### 3. Publishing State (`async_publish`)
Used to send updates to Home Assistant.

```cpp
template<typename CompletionToken>
auto async_publish(std::string_view state, CompletionToken&& token);
```

### 4. Handling Commands (`async_recv` - Internal/Button specific)
For entities like `Button`, the `async_run` loop will internally spawn an `async_recv` loop to handle incoming MQTT messages and invoke user-provided callbacks or update internal state.

## Cross-Thread Integration

The library must safely support calls from external threads (e.g., a REST API running on a separate thread pool) without causing data races. This is achieved via Asio's execution context guarantees.

**Scenario 1: Fire-and-forget from an external thread**
```cpp
// REST thread posting to the entity's strand
boost::asio::post(sensor.get_executor(), [&sensor, new_state]() {
    // Safe to interact with sensor here
    sensor.publish_nowait(new_state); // Non-blocking, best-effort queueing
});
```

**Scenario 2: Synchronous wait from an external thread**
Using `boost::asio::co_spawn` and `boost::asio::use_future` to bridge the asynchronous entity strand with a blocking external thread.
```cpp
// REST thread
std::future<void> fut = boost::asio::co_spawn(
    sensor.get_executor(),
    sensor.async_publish(new_state, boost::asio::use_awaitable),
    boost::asio::use_future
);

try {
    fut.get(); // Blocks until publish completes or fails
} catch(const std::exception& e) {
    // Handle publish failure
}
```

## Implementation Phasing
1. **Phase 1:** Implement `ResilientMqttClient` with connection, auto-reconnect, and strand-based thread safety.
2. **Phase 2:** Implement the base Entity logic (`async_run`, `async_config`).
3. **Phase 3:** Implement concrete `BinarySensor`, `Sensor`, and `Button` with their specific publish/receive requirements.
4. **Phase 4:** Thoroughly test concurrent access, dropped connections, and backoff logic using simulated broker failures.
