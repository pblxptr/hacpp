#include <async_mqtt/util/log_severity.hpp>
#include <async_mqtt/util/setup_log.hpp>

#include <catch2/catch_session.hpp>
#include <spdlog/common.h>
#include <spdlog/spdlog.h>

// #include "helpers/test_setup.hpp"
// #include "helpers/test_config.hpp"
// NOLINTNEXTLINE(misc-use-internal-linkage): weak hook must keep external linkage so tests can override it.
__attribute__((weak)) void test_spec_setup()
{
  //  throw std::runtime_error{ "dupa" };
}

int main(int argc, char* argv[])
{
  async_mqtt::setup_log(async_mqtt::severity_level::trace);
  auto session = Catch::Session{};

  // auto& config = TestConfig::get();
  // test_spec_setup();
  // config.apply(session);

  spdlog::set_level(spdlog::level::debug);

  int return_code = session.applyCommandLine(argc, argv);
  if (return_code != 0) { // Indicates a command line error
    return return_code;
  }

  return session.run(argc, argv);
}
