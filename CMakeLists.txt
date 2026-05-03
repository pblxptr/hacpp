cmake_minimum_required(VERSION 3.27)

project(hacpp VERSION 0.1.0 LANGUAGES CXX)

if(PROJECT_IS_TOP_LEVEL)
  include(cmake/compilation-flags.cmake)
  include(cmake/sanitizers.cmake)
endif()

include(cmake/async_mqtt.cmake)

add_subdirectory(lib)

# if (ENABLE_TESTING)
  enable_testing()
  add_subdirectory(test)
# endif(ENABLE_TESTING)
