# hacpp (Home Assistant C++)

[![Build](https://github.com/pblxptr/hacpp/actions/workflows/build.yml/badge.svg)](https://github.com/pblxptr/hacpp/actions/workflows/build.yml)
[![Tests](https://github.com/pblxptr/hacpp/actions/workflows/tests.yml/badge.svg)](https://github.com/pblxptr/hacpp/actions/workflows/tests.yml)
[![Quality](https://github.com/pblxptr/hacpp/actions/workflows/quality.yml/badge.svg)](https://github.com/pblxptr/hacpp/actions/workflows/quality.yml)

hacpp is a resilient, thread-safe C++ library built on top of `async_mqtt` and `Boost.Asio` designed to manage Home Assistant entities via MQTT. It provides a modern, asynchronous API using C++23 coroutines, handling the complexities of connection management and Home Assistant's MQTT Discovery protocol.

The library is in early stage of development so basiscally everyting can be changed soon.

## Features

- **Resilient Connectivity:** Automatic reconnection with backoff logic and message buffering during outages.
- **Thread-Safe:** Leverages Boost.Asio strands to ensure sequential state mutations without explicit locking.
- **HA Entity Support:** High-level abstractions for common entities:
    - **Binary Sensor:** For reporting on/off states.
    - **Sensor:** For reporting numerical or string values.
    - **Button:** For receiving "press" commands from Home Assistant.
    - **Cover:** For controlling blinds, doors, or shutters.
- **Modern C++:** Designed for C++23, utilizing coroutines (`boost::asio::awaitable`) for clean asynchronous code.
- **MQTT v5:** Full support for MQTT v5 features via `async_mqtt`.

## Requirements

- **C++20 compatible compiler:** GCC 14+, Clang 19+
- **CMake:** 3.27+
- **Conan:** 2.x
- **Boost:** 1.90.0+

## Getting Started

### Building from Source

The project uses CMake presets and Conan for dependency management. Conan is automatically invoked by CMake during the configuration step.

1. **Clone the repository:**
   ```bash
   git clone https://github.com/pblxptr/hacpp.git
   cd hacpp
   ```

2. **Configure the project:**
   Choose a preset from `CMakePresets.json`. For example, using GCC in debug mode:
   ```bash
   cmake --preset linux-native-conan-gcc-debug
   ```

3. **Build:**
   ```bash
   cmake --build build
   ```

### Running Tests

Tests are managed with Catch2. Integration tests run within a Docker environment to simulate an MQTT broker.

1. **Unit Tests:**
   After building, you can run tests via CTest:
   ```bash
   cd build
   ctest
   ```

2. **Integration Tests:**
   The project uses `docker-compose` to run integration tests against a real MQTT broker:
   ```bash
   cd tests/integration_tests/env
   export ARTIFACTS_DIR=../../../build/bin/
   docker compose up --build --exit-code-from test test
   ```

## Development

### Code Quality

We use `clang-format` and `clang-tidy` to maintain code quality. Helper scripts are provided in the `tools/` directory:

- **Formatting:** `./tools/check-clang-format.sh`
- **Linting:** `cd build && ../tools/check-clang-tidy.sh`

### Dev Containers

The project includes configurations for Visual Studio Code Dev Containers, providing a consistent development environment with all necessary tools (GCC 14, Clang 19, Conan, etc.) pre-installed.

## License

This project is licensed under the [MIT License](LICENSE) (placeholder).
