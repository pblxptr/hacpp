cmake_minimum_required(VERSION 3.27)

project(hacpp VERSION 0.1.0 LANGUAGES CXX)

if(PROJECT_IS_TOP_LEVEL)
  include(cmake/compilation-flags.cmake)
  include(cmake/sanitizers.cmake)
endif()

include(cmake/async_mqtt.cmake)

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

add_subdirectory(lib)

enable_testing()
add_subdirectory(tests)
