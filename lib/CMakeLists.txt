
find_package(spdlog REQUIRED)
find_package(Boost REQUIRED COMPONENTS json)


add_library(hacpp INTERFACE)

target_include_directories(hacpp INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

target_link_libraries(hacpp INTERFACE
  async_mqtt_iface
  spdlog::spdlog
  Boost::json
)

target_compile_definitions(hacpp INTERFACE
  BOOST_ASIO_HAS_STD_COROUTINE
  BOOST_ASIO_HAS_CO_AWAIT
)

