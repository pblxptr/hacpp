include(FetchContent)

FetchContent_Declare(
    async_mqtt-src
    GIT_REPOSITORY https://github.com/redboltz/async_mqtt
    GIT_TAG 10.3.0
)

FetchContent_MakeAvailable(async_mqtt-src)
