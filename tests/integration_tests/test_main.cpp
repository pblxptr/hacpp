#include <async_mqtt/all.hpp>
#include <catch2/catch_session.hpp>
#include <spdlog/spdlog.h>

// #include "helpers/test_setup.hpp"
// #include "helpers/test_config.hpp"

__attribute__((weak)) void test_spec_setup() {
  //  throw std::runtime_error{ "dupa" };
}

int main(int argc, char *argv[]) {
  async_mqtt::setup_log(async_mqtt::severity_level::trace);
  using namespace Catch::Clara;

  auto session = Catch::Session{};

  // auto& config = TestConfig::get();
  // test_spec_setup();
  // config.apply(session);

  spdlog::set_level(spdlog::level::debug);

  int returnCode = session.applyCommandLine(argc, argv);
  if (returnCode != 0) // Indicates a command line error
    return returnCode;

  return session.run(argc, argv);
}
