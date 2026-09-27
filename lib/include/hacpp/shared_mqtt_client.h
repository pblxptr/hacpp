#pragma once

#include <hacpp/async_mqtt_client.h>
#include <hacpp/logger.h>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <algorithm>
#include <deque>
#include <memory>
#include <utility>
#include <vector>

namespace hacpp::mqtt {

class SharedAsyncMqttConnection;

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
      detail::logger()->debug("RecvResultQueue::{}:", __func__);

      queue_.push_back(std::move(result));
      timer_.cancel();
      co_return;
    }

    boost::asio::awaitable<RecvResult> pop_front()
    {
      detail::logger()->debug("RecvResultQueue::{}:", __func__);

      boost::system::error_code ec;

      if (queue_.empty()) {
        co_await timer_.async_wait(boost::asio::redirect_error(boost::asio::use_awaitable, ec));
      }

      if (queue_.empty()) {
        if (ec) {
          co_return std::unexpected(map_err(ec));
        }

        detail::logger()->error("Wait timer has been cancelled but queue is still empty and timer finished with no error");
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

class SharedClientState
{
  public:
    explicit SharedClientState(const boost::asio::any_io_executor& executor)
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

class SharedAsyncMqttClient
{
  public:
    explicit SharedAsyncMqttClient(std::shared_ptr<SharedAsyncMqttConnection> shared_connection);
    SharedAsyncMqttClient(const SharedAsyncMqttClient&) = delete;
    SharedAsyncMqttClient& operator=(const SharedAsyncMqttClient&) = delete;
    SharedAsyncMqttClient(SharedAsyncMqttClient&&) noexcept = default;
    SharedAsyncMqttClient& operator=(SharedAsyncMqttClient&&) noexcept = default;
    ~SharedAsyncMqttClient() = default;

    auto executor();
    boost::asio::awaitable<Error> async_close();
    template <typename... Args>
    boost::asio::awaitable<Error> async_publish(Args... args);
    template <typename... Args>
    boost::asio::awaitable<Error> async_subscribe(Args... args);
    boost::asio::awaitable<RecvResult> async_recv();

  private:
    std::shared_ptr<SharedAsyncMqttConnection> shared_connection_;
    std::shared_ptr<SharedClientState> state_;
};

class SharedAsyncMqttConnection : public std::enable_shared_from_this<SharedAsyncMqttConnection>
{
    explicit SharedAsyncMqttConnection(AsyncMqttClient client)
        : client_(std::move(client))
    {}

  public:
    SharedAsyncMqttConnection(const SharedAsyncMqttConnection&) = delete;
    SharedAsyncMqttConnection& operator=(const SharedAsyncMqttConnection&) = delete;
    SharedAsyncMqttConnection(SharedAsyncMqttConnection&&) = delete;
    SharedAsyncMqttConnection& operator=(SharedAsyncMqttConnection&&) = delete;
    ~SharedAsyncMqttConnection() = default;

    static std::shared_ptr<SharedAsyncMqttConnection> create(AsyncMqttClient client)
    {
      return std::shared_ptr<SharedAsyncMqttConnection>(new SharedAsyncMqttConnection(std::move(client)));
    }

    auto executor()
    {
      return client_.executor();
    }

    auto make_client()
    {
      return SharedAsyncMqttClient{shared_from_this()};
    }

    boost::asio::awaitable<Error> async_connect()
    {
      return client_.async_connect();
    }

    boost::asio::awaitable<Error> async_close()
    {
      co_return co_await client_.async_close();
    }

    boost::asio::awaitable<void> async_run()
    {
      while (true) {
        auto err = co_await async_pump_one_impl();
        if (err) {
          co_return;
        }
      }
    }

    boost::asio::awaitable<Error> async_detach(std::shared_ptr<SharedClientState> state)
    {
      // TODO(pbiel): Consider unsubscribing from topics owned only by this logical shared client.
      std::erase_if(clients_, [&state](const std::weak_ptr<SharedClientState>& weak_client) {
        return weak_client.lock() == state;
      });

      co_return Error{};
    }

    template <typename... Args>
    boost::asio::awaitable<Error> async_publish(Args... args)
    {
      co_return co_await client_.async_publish(std::move(args)...);
    }

    boost::asio::awaitable<Error>
    async_subscribe(std::shared_ptr<SharedClientState> state, std::vector<TopicSubopts> topics)
    {
      auto err = co_await client_.async_subscribe(topics);
      if (err) {
        co_return err;
      };

      auto& registered_topics = state->topics();
      for (const auto& topic : topics) {
        auto registered_topic = std::ranges::find_if(
            registered_topics,
            [&topic](const TopicSubopts& registered) { return registered.topic() == topic.topic(); });

        if (registered_topic == registered_topics.end()) {
          registered_topics.push_back(topic);
        } else {
          *registered_topic = topic;
        }
      }

      const auto registered_client = std::ranges::any_of(
          clients_,
          [&state](const std::weak_ptr<SharedClientState>& weak_client) { return weak_client.lock() == state; });

      if (!registered_client) {
        clients_.push_back(state);
      }

      co_return err;
    }

    boost::asio::awaitable<RecvResult> async_recv(std::shared_ptr<SharedClientState> state)
    {
      const auto registered_client = std::ranges::any_of(
          clients_,
          [&state](const std::weak_ptr<SharedClientState>& weak_client) { return weak_client.lock() == state; });

      if (!registered_client) {
        clients_.push_back(state);
      }

      co_return co_await state->queue().pop_front();
    }

    boost::asio::awaitable<void> async_pump_one()
    {
      co_await async_pump_one_impl();
    }

  private:
    boost::asio::awaitable<Error> async_pump_one_impl()
    {
      remove_expired_clients();

      auto result = co_await client_.async_recv();

      if (!result) {
        detail::logger()->debug(
            "Received error in shared MQTT connection pump, forwarding to all clients: {}",
            result.error().message());

        for (const auto& weak_client : clients_) {
          if (auto client = weak_client.lock()) {
            detail::logger()->debug("Forwarding error to shared client");
            co_await client->queue().push_back(result);
          } else {
            detail::logger()->debug("Shared client has expired, skipping");
          }
        }
        co_return result.error() == ErrorCode::SessionReset ? ErrorCode::Success : result.error();
      }

      const auto* packet = result->get_if<PublishPacket>();
      if (packet == nullptr) {
        result->visit([](auto&& packet) {
          detail::logger()->warn(
              "Received non-publish packet in shared MQTT connection pump, skipping: {}",
              detail::str(packet));
        });
        co_return ErrorCode::Success;
      }

      // TODO(pbiel): This is a naive implementation that iterates through all shared clients and their topics for every
      // received packet.
      for (const auto& weak_client : clients_) {
        if (auto client = weak_client.lock()) {
          for (const auto& topic : client->topics()) {
            if (topic.topic() == packet->topic()) {
              co_await client->queue().push_back(result);
            }
          }
        }
      }

      co_return ErrorCode::Success;
    }

    void remove_expired_clients()
    {
      std::erase_if(
          clients_,
          [](const std::weak_ptr<SharedClientState>& weak_client) { return weak_client.expired(); });
    }

    AsyncMqttClient client_;
    std::vector<std::weak_ptr<SharedClientState>> clients_;
};

inline SharedAsyncMqttClient::SharedAsyncMqttClient(std::shared_ptr<SharedAsyncMqttConnection> shared_connection)
    : shared_connection_(std::move(shared_connection))
    , state_{std::make_shared<SharedClientState>(shared_connection_->executor())}
{}

inline auto SharedAsyncMqttClient::executor()
{
  return shared_connection_->executor();
}

inline boost::asio::awaitable<Error> SharedAsyncMqttClient::async_close()
{
  co_return co_await shared_connection_->async_detach(state_);
}

template <typename... Args>
boost::asio::awaitable<Error> SharedAsyncMqttClient::async_publish(Args... args)
{
  co_return co_await shared_connection_->async_publish(std::move(args)...);
}

template <typename... Args>
boost::asio::awaitable<Error> SharedAsyncMqttClient::async_subscribe(Args... args)
{
  co_return co_await shared_connection_->async_subscribe(state_, std::move(args)...);
}

inline boost::asio::awaitable<RecvResult> SharedAsyncMqttClient::async_recv()
{
  co_return co_await shared_connection_->async_recv(state_);
}

} // namespace hacpp::mqtt
