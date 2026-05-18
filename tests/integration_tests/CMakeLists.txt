add_executable(hacpp-integration-tests
  test_main.cpp
  # test_spec_setup.cpp
  client_tests.cpp
  binary_sensor.cpp
  sensor.cpp
  button.cpp
  cover.cpp
)

find_package(Boost REQUIRED COMPONENTS log filesystem)
find_package(Catch2 REQUIRED)
find_package(spdlog REQUIRED)

target_include_directories(hacpp-integration-tests PRIVATE .)
target_compile_definitions(hacpp-integration-tests PRIVATE 
    INTEGRATION_TEST_ENV_DIR="${CMAKE_CURRENT_SOURCE_DIR}/env"
)

target_link_libraries(hacpp-integration-tests
  PRIVATE
    Boost::log
    Boost::filesystem
    Catch2::Catch2
    spdlog::spdlog
    hacpp
)

# target_compile_options(hacpp-integration-tests PRIVATE --coverage)
# target_link_options(hacpp-integration-tests PRIVATE --coverage)

# add_test(NAME hacpp_integration_tests COMMAND hacpp-integration-tests)

# target_compile_definitions(hacpp-integration-tests PRIVATE ASYNC_MQTT_USE_LOG)
