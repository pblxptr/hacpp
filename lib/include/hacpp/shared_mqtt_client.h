#pragma once

#include <hacpp/async_mqtt_client.h>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <spdlog/spdlog.h>

#include <deque>
#include <memory>
#include <utility>
#include <vector>

namespace hacpp::mqtt {

class SharedAsyncMqttClient;

class RecvResultQueue
{
  public:
    explicit RecvResultQueue(const boost::asio::any_io_executor& executor)
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

      boost::system::error_code ec;

      if (queue_.empty()) {
        co_await timer_.async_wait(boost::asio::redirect_error(boost::asio::use_awaitable, ec));
      }

      if (queue_.empty()) {
        if (ec) {
          co_return std::unexpected(map_err(ec));
        }

        spdlog::error("Wait timer has been cancelled but queue is still empty and timer finished with no error");
        co_return std::unexpected(ErrorCode::InternalError);
      }

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
    explicit ProxyState(const boost::asio::any_io_executor& executor)
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
    explicit SharedAsyncMqttClient(AsyncMqttClient2 client)
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
      std::erase_if(proxies_, [&state](const std::weak_ptr<ProxyState>& weak_proxy) {
        return weak_proxy.lock() == state;
      });

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
        co_return;
      }

      const auto& packet = result->get<PublishPacket>();

      // TODO(pbiel): This is a naive implementation that iterates through all proxies and their topics for every
      // received packet.
      for (const auto& weak_proxy : proxies_) {
        if (auto proxy = weak_proxy.lock()) {
          for (const auto& topic : proxy->topics()) {
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

inline SharedClientProxy::SharedClientProxy(std::shared_ptr<SharedAsyncMqttClient> shared_client)
    : shared_client_(std::move(shared_client))
    , state_{std::make_shared<ProxyState>(shared_client_->executor())}
{}

inline auto SharedClientProxy::executor()
{
  return shared_client_->executor();
}

inline boost::asio::awaitable<Error> SharedClientProxy::async_close()
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

inline boost::asio::awaitable<RecvResult> SharedClientProxy::async_recv()
{
  co_return co_await state_->queue().pop_front();
}

} // namespace hacpp::mqtt
