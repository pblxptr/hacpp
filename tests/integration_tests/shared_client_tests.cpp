#include <hacpp/async_mqtt_client.h>
#include <catch2/catch_test_macros.hpp>

namespace hacpp::mqtt {

class SharedAsyncMqttClient;

class RecvResultQueue
{
public:
    RecvResultQueue(boost::asio::any_io_executor executor)
        : timer_{executor}
    {
        timer_.expires_at(boost::asio::steady_timer::time_point::max());
    }

    boost::asio::awaitable<void> push_back(RecvResult result)
    {
        spdlog::debug("RecvResultQueue::{}:", __func__);

        queue_.push_back(std::move(result));

        timer_.cancel();
        co_return;
    }

    boost::asio::awaitable<RecvResult> pop_front()
    {
        spdlog::debug("RecvResultQueue::{}:", __func__);

        if (queue_.empty()) {
            spdlog::debug("RecvResultQueue::{}: waiting...", __func__);
            boost::system::error_code ec;
            co_await timer_.async_wait(boost::asio::redirect_error(boost::asio::use_awaitable, ec));
            spdlog::debug("RecvResultQueue::{}: waiting done", __func__);
        }

        // TODO: Check if queue has an element

        auto result = std::move(queue_.front());
        queue_.pop_front();
        co_return result;
    }

private:
    std::deque<RecvResult> queue_;
    boost::asio::steady_timer timer_;
};

class ProxyState
{
public:
    ProxyState(boost::asio::any_io_executor executor)
        : queue_(executor)
    {}

    RecvResultQueue& queue()
    {
        return queue_;
    }

    std::vector<TopicSubopts>& topics()
    {
        return topics_;
    }

private:
    RecvResultQueue queue_;
    std::vector<TopicSubopts> topics_;
};

class SharedClientProxy
{
public:
    explicit SharedClientProxy(std::shared_ptr<SharedAsyncMqttClient> shared_client);
    auto executor();
    boost::asio::awaitable<Error> async_close();
    template <typename... Args>
    boost::asio::awaitable<Error> async_publish(Args... args);
    template <typename... Args>
    boost::asio::awaitable<Error> async_subscribe(Args... args);
    boost::asio::awaitable<RecvResult> async_recv();
private:
    std::shared_ptr<SharedAsyncMqttClient> shared_client_;
    std::shared_ptr<ProxyState> state_;
};

class SharedAsyncMqttClient : public std::enable_shared_from_this<SharedAsyncMqttClient>
{
private:
    SharedAsyncMqttClient(AsyncMqttClient2 client)
        : client_(std::move(client))
    {}

public:
    static std::shared_ptr<SharedAsyncMqttClient> create(AsyncMqttClient2 client)
    {
        return std::shared_ptr<SharedAsyncMqttClient>(new SharedAsyncMqttClient(std::move(client)));
    }

    auto executor()
    {
        return client_.executor();
    }

    auto proxy()
    {
        return SharedClientProxy{shared_from_this()};
    }

    boost::asio::awaitable<Error> async_connect()
    {
        return client_.async_connect();
    }

    boost::asio::awaitable<Error> async_close(std::shared_ptr<ProxyState> state)
    {
        // Remove the proxy state from the list of proxies
        proxies_.erase(std::remove_if(proxies_.begin(), proxies_.end(),
            [&state](const std::weak_ptr<ProxyState>& weak_proxy) {
                return weak_proxy.lock() == state;
            }), proxies_.end());

        co_return Error{};
    }

    template <typename... Args>
    boost::asio::awaitable<Error> async_publish(Args... args)
    {
      co_return co_await client_.async_publish(std::move(args)...);
    }

    boost::asio::awaitable<Error> async_subscribe(std::shared_ptr<ProxyState> state, std::vector<TopicSubopts> topics)
    {
        auto err = co_await client_.async_subscribe(topics);
        if (err) {
            co_return err;
        };

        for (const auto& topic : topics) {
            state->topics().push_back(topic);
        }

        proxies_.push_back(state);

        spdlog::debug("SharedAsyncMqttClient::{}: proxies count: {}", __func__, proxies_.size());

        co_return err;
    }

    boost::asio::awaitable<void> async_recv()
    {
        auto result = co_await client_.async_recv();

        if (!result) {
            for (const auto& weak_proxy : proxies_) {
                if (auto proxy = weak_proxy.lock()) {
                    co_await proxy->queue().push_back(result);
                }
            }
        }

        const auto& packet = result->get<PublishPacket>();

        spdlog::debug("SharedAsyncMqttClient::{}: proxies count: {}", __func__, proxies_.size());

        // TODO: This is a naive implementation that iterates through all proxies and their topics for every received packet.
        for (const auto& weak_proxy : proxies_) {
            if (auto proxy = weak_proxy.lock()) {
                spdlog::debug("Proxy is alive");
                for (const auto& topic : proxy->topics()) {
                    spdlog::debug("Pushing to proxy...");
                    if (topic.topic() == packet.topic()) {
                        co_await proxy->queue().push_back(result);
                    }
                }
            }
        }
    }

private:
    AsyncMqttClient2 client_;
    std::vector<std::weak_ptr<ProxyState>> proxies_;
};


SharedClientProxy::SharedClientProxy(std::shared_ptr<SharedAsyncMqttClient> shared_client)
    : shared_client_(std::move(shared_client))
    , state_{std::make_shared<ProxyState>(shared_client_->executor())}
{}

auto SharedClientProxy::executor()
{
    return shared_client_->executor();
}

boost::asio::awaitable<Error> SharedClientProxy::async_close()
{
    co_return co_await shared_client_->async_close(state_);
}

template <typename... Args>
boost::asio::awaitable<Error> SharedClientProxy::async_publish(Args... args)
{
    co_return co_await shared_client_->async_publish(std::move(args)...);
}

template <typename... Args>
boost::asio::awaitable<Error> SharedClientProxy::async_subscribe(Args... args)
{
    co_return co_await shared_client_->async_subscribe(state_, std::move(args)...);
}

boost::asio::awaitable<RecvResult> SharedClientProxy::async_recv()
{
    co_return co_await state_->queue().pop_front();
}

}

/*

Entity1 ---- Proxy -
                   |
                   |
Entity2 ---- Proxy ------ SharedClient ----- Client
                   |
                   |
Entity3 ---- Proxy -

*/

#include "config.h"
#include <hacpp/button.h>
#include <random>

using hacpp::mqtt::SharedClientProxy;
using hacpp::mqtt::Button;
using hacpp::mqtt::ButtonCfg;
using hacpp::mqtt::ClientType;
using hacpp::mqtt::factory;
using hacpp::mqtt::default_component_command_topic;

#include <iostream>

auto button(std::string id, SharedClientProxy proxy)
{
    return factory<Button>(id, std::move(proxy))
        .set(ButtonCfg::Opt::PayloadPress, fmt::format("press-{}", id))
        .on_press([id = std::move(id)]() -> boost::asio::awaitable<void> {
            std::cout << "id:" << id << '\n';
            co_return;
        })
        .create();
}

static constexpr auto NumberOfButtonEntities = 50;
static constexpr auto NumberOfPublishPerEntity = 100;
static constexpr auto TotalMsgExchange = NumberOfButtonEntities * NumberOfPublishPerEntity;
static constexpr auto ExchangeTimeout = std::chrono::seconds{180};

static std::atomic<bool> PublishDone = false;

static auto get_payload_press(const std::string& id)
{
    return fmt::format("press-{}", id);
}

static auto get_id_str(int id)
{
    return fmt::format("btn-{}", id);
}

static auto get_command_topic(const std::string& id)
{
    return default_component_command_topic(ButtonCfg::Defs::Component, id);
}

static auto spawn_publisher_thread()
{
    auto io = std::make_shared<boost::asio::io_context>();
    auto strand = boost::asio::make_strand(*io);

    auto publist = std::vector<int>{};
    for (auto i = 0; i < NumberOfButtonEntities; i++) {
        for (auto j = 0; j < NumberOfPublishPerEntity; j++) {
            publist.push_back(i);
        }
    }

    std::shuffle(publist.begin(), publist.end(), std::mt19937{std::random_device{}()});

    boost::asio::co_spawn(strand, [io, strand, publist = std::move(publist)](this auto /* self */) -> boost::asio::awaitable<void> {
        auto client = std::make_shared<ClientType>(strand, config());
        auto err = co_await client->async_connect();
        REQUIRE(!err);

        for (const auto& id : publist) {
            const auto id_str = get_id_str(id);
            const auto topic = get_command_topic(id_str);
            const auto payload = get_payload_press(id_str);

            auto err = co_await client->async_publish(topic, payload);
            REQUIRE(!err);
            spdlog::debug("topic: {}, payload: {}", topic, payload);
        }
        io->stop();
    }, boost::asio::detached);

    return std::jthread([io]() {
        std::this_thread::sleep_for(std::chrono::seconds{10});

        io->run();
        PublishDone = true;
        spdlog::debug("Publish done");
    });
}

TEST_CASE("SharedAsyncMqttClient can handle multiple entities", "[integration][shared_async_mqtt_client][robustness]")
{
    // Arrange
    static auto Counters = std::unordered_map<std::string, int>{};
    for (int i = 0; i < NumberOfButtonEntities; ++i) {
        Counters[get_id_str(i)] = 0;
    }

    auto io = boost::asio::io_context{};
    auto strand = boost::asio::make_strand(io);

    // Act
    boost::asio::co_spawn(strand, [strand](this auto /* self */) -> boost::asio::awaitable<void> {
        auto shared_client = hacpp::mqtt::SharedAsyncMqttClient::create(
            hacpp::mqtt::AsyncMqttClient2{strand, config()});

        auto err = co_await shared_client->async_connect();
        REQUIRE(!err);

        boost::asio::co_spawn(shared_client->executor(), [shared_client](this auto /* self */) -> boost::asio::awaitable<void> {
            while (true) {
                co_await shared_client->async_recv();
            }
        }, boost::asio::detached);

        for (int i = 0; i < NumberOfButtonEntities; ++i) {
            boost::asio::co_spawn(shared_client->executor(), [shared_client, i](this auto /* self */) -> boost::asio::awaitable<void>{
                auto id = get_id_str(i);
                auto btn = factory<Button>(get_id_str(i), shared_client->proxy())
                    .set(ButtonCfg::Opt::PayloadPress, get_payload_press(id))
                    .set(ButtonCfg::Opt::CommandTopic, get_command_topic(id))
                    .on_press([id = std::move(id)]() -> boost::asio::awaitable<void> {
                        Counters[id]++;
                        co_return;
                    })
                    .create();
                co_await btn.async_setup();
                co_await btn.async_run();
            }, boost::asio::detached);
        }
    }, boost::asio::detached);

    boost::asio::co_spawn(strand, [strand, &io](this auto /* self */) -> boost::asio::awaitable<void> {
        auto timer = boost::asio::steady_timer{strand};
        auto start = boost::asio::steady_timer::clock_type::now();
        auto end = boost::asio::steady_timer::clock_type::now() + std::chrono::seconds{TotalMsgExchange};

        auto accumulate = []() {
            return std::accumulate(Counters.begin(), Counters.end(), 0, [](int acc, auto& elem) {
                return acc + elem.second;
            });
        };

        while (true) {
            if (accumulate() == TotalMsgExchange) {
                break;
            }

            if (boost::asio::steady_timer::clock_type::now() < end) {
                spdlog::debug("Timeout still valid... waiting");
                timer.expires_after(std::chrono::seconds{1});
                co_await timer.async_wait(boost::asio::use_awaitable);
            } else {
                spdlog::debug("Timer expired...stopping");
                break;
            }
        }

        io.stop();
    }, boost::asio::detached);

    auto publisher_th = spawn_publisher_thread();

    io.run();

    // Assert
    REQUIRE(PublishDone);
    int total_msgs = std::accumulate(Counters.begin(), Counters.end(), 0, [](int acc, auto& elem) {
        return acc + elem.second;
    });

    REQUIRE(total_msgs == TotalMsgExchange);
    for (const auto& [id, count] : Counters) {
        REQUIRE(count == NumberOfPublishPerEntity);
    }
}

TEST_CASE("SharedAsyncMqttClient can be constructed and destructed", "[integration][shared_async_mqtt_client]")
{
    auto io = boost::asio::io_context{};
    auto strand = boost::asio::make_strand(io);

    boost::asio::co_spawn(strand, [strand](this auto /* self */) -> boost::asio::awaitable<void> {

        auto shared_client = hacpp::mqtt::SharedAsyncMqttClient::create(
            hacpp::mqtt::AsyncMqttClient2{strand, config()});

        auto err = co_await shared_client->async_connect();
        REQUIRE(!err);

        boost::asio::co_spawn(shared_client->executor(), [shared_client](this auto /* self */) -> boost::asio::awaitable<void> {
            while (true) {
                co_await shared_client->async_recv();
            }
        }, boost::asio::detached);

        boost::asio::co_spawn(shared_client->executor(), [shared_client](this auto /* self */) -> boost::asio::awaitable<void>{
            auto btn1 = button("btn-1", shared_client->proxy());
            co_await btn1.async_setup();
            co_await btn1.async_run();
        }, boost::asio::detached);

        boost::asio::co_spawn(shared_client->executor(), [shared_client](this auto /* self */) -> boost::asio::awaitable<void>{
            auto btn2 = button("btn-2", shared_client->proxy());
            co_await btn2.async_setup();
            co_await btn2.async_run();
        }, boost::asio::detached);

        boost::asio::co_spawn(shared_client->executor(), [shared_client](this auto /* self */) -> boost::asio::awaitable<void>{
            auto btn3 = button("btn-3", shared_client->proxy());
            co_await btn3.async_setup();
            co_await btn3.async_run();
        }, boost::asio::detached);

    }, boost::asio::detached);

    io.run();
}
