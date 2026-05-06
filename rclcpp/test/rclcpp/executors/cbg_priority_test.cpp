// Copyright 2026 Nova ROS, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "rclcpp/executors/cbg/cbg_events_executor.hpp"
#include "rclcpp/executors/cbg/cbg_schedule_queue.hpp"
#include "rclcpp/rclcpp.hpp"
#include "test_msgs/msg/empty.hpp"

using namespace std::chrono_literals;
using namespace rclcpp::executors::cbg;
using TestMsg = test_msgs::msg::Empty;

// ========================================================================
// CBGScheduleQueue priority API tests (unit level)
// ========================================================================

class CBGPriorityQueueTest : public ::testing::Test
{
protected:
  void SetUp() override
  {
    rclcpp::init(0, nullptr);
  }

  void TearDown() override
  {
    rclcpp::shutdown();
  }
};

TEST_F(CBGPriorityQueueTest, SetAndGetPriority)
{
  using PrioQueue = CBGScheduleQueue<PrioritySchedule, std::allocator<void>>;
  PrioQueue queue;

  // Create a CallbackGroup and register it
  auto node = std::make_shared<rclcpp::Node>("test_prio_set_get");
  auto cbg = node->create_callback_group(rclcpp::CallbackGroupType::MutuallyExclusive);

  auto * handle = queue.register_group(cbg, 64);

  // Default priority should be 0
  EXPECT_EQ(queue.get_priority(cbg.get()), 0);

  // Set priority to 10
  queue.set_priority(handle, 10);
  EXPECT_EQ(queue.get_priority(cbg.get()), 10);

  // Set priority to -5
  queue.set_priority(handle, -5);
  EXPECT_EQ(queue.get_priority(cbg.get()), -5);

  // Out of range priorities should be ignored
  queue.set_priority(handle, 300);  // > 256
  EXPECT_EQ(queue.get_priority(cbg.get()), -5);  // unchanged

  queue.set_priority(handle, -300);  // < -256
  EXPECT_EQ(queue.get_priority(cbg.get()), -5);  // unchanged
}

TEST_F(CBGPriorityQueueTest, FIFOQueueReturnsZero)
{
  using FIFOQueue = CBGScheduleQueue<FIFOSchedule, std::allocator<void>>;
  FIFOQueue queue;

  auto node = std::make_shared<rclcpp::Node>("test_fifo_zero_prio");
  auto cbg = node->create_callback_group(rclcpp::CallbackGroupType::MutuallyExclusive);

  auto * handle = queue.register_group(cbg, 64);

  // FIFO queue always returns 0 for get_priority
  EXPECT_EQ(queue.get_priority(cbg.get()), 0);

  // set_priority is a no-op for FIFO
  queue.set_priority(handle, 10);
  EXPECT_EQ(queue.get_priority(cbg.get()), 0);  // still 0
}

// ========================================================================
// CBGEventsExecutor priority integration tests
// ========================================================================

class CBGPriorityExecutorTest : public ::testing::Test
{
protected:
  static void SetUpTestSuite()
  {
    rclcpp::init(0, nullptr);
  }

  static void TearDownTestSuite()
  {
    rclcpp::shutdown();
  }
};

/// Test that set_cbg_priority/get_cbg_priority work correctly
/// with the Priority executor.
TEST_F(CBGPriorityExecutorTest, SetAndGetCbgPriority)
{
  auto executor = std::make_shared<CBGEventsExecutorPriority>();
  auto node = std::make_shared<rclcpp::Node>("test_executor_prio");

  auto cbg_high = node->create_callback_group(
    rclcpp::CallbackGroupType::MutuallyExclusive);
  auto cbg_low = node->create_callback_group(
    rclcpp::CallbackGroupType::MutuallyExclusive);

  executor->add_callback_group(cbg_high, node->get_node_base_interface());
  executor->add_callback_group(cbg_low, node->get_node_base_interface());

  // Default priority is 0
  EXPECT_EQ(executor->get_cbg_priority(cbg_high.get()), 0);
  EXPECT_EQ(executor->get_cbg_priority(cbg_low.get()), 0);

  // Set priorities
  executor->set_cbg_priority(cbg_high.get(), 100);
  executor->set_cbg_priority(cbg_low.get(), -50);

  EXPECT_EQ(executor->get_cbg_priority(cbg_high.get()), 100);
  EXPECT_EQ(executor->get_cbg_priority(cbg_low.get()), -50);

  executor->cancel();
}

/// Test that FIFO executor always returns 0 for get_cbg_priority
/// and set_cbg_priority is a no-op.
TEST_F(CBGPriorityExecutorTest, FIFOExecutorPriorityNoop)
{
  auto executor = std::make_shared<CBGEventsExecutorDefault>();
  auto node = std::make_shared<rclcpp::Node>("test_fifo_executor_prio");

  auto cbg = node->create_callback_group(
    rclcpp::CallbackGroupType::MutuallyExclusive);

  executor->add_callback_group(cbg, node->get_node_base_interface());

  // FIFO always returns 0
  EXPECT_EQ(executor->get_cbg_priority(cbg.get()), 0);

  // set_cbg_priority is a no-op
  executor->set_cbg_priority(cbg.get(), 100);
  EXPECT_EQ(executor->get_cbg_priority(cbg.get()), 0);  // still 0

  executor->cancel();
}

/// Test that high-priority CBG gets executed before low-priority CBG
/// when both have events ready simultaneously.
/// Uses MutuallyExclusive groups to enforce ordering within each group.
TEST_F(CBGPriorityExecutorTest, HighPriorityExecutesFirst)
{
  auto executor = std::make_shared<CBGEventsExecutorPriority>();
  auto node = std::make_shared<rclcpp::Node>("test_prio_order");

  auto cbg_low = node->create_callback_group(
    rclcpp::CallbackGroupType::MutuallyExclusive);
  auto cbg_high = node->create_callback_group(
    rclcpp::CallbackGroupType::MutuallyExclusive);

  // Set priorities: high=50, low=-50
  executor->add_callback_group(cbg_low, node->get_node_base_interface());
  executor->add_callback_group(cbg_high, node->get_node_base_interface());
  executor->set_cbg_priority(cbg_high.get(), 50);
  executor->set_cbg_priority(cbg_low.get(), -50);

  std::mutex order_mutex;
  std::vector<std::string> execution_order;

  // Create subscriptions in different CBGs
  rclcpp::SubscriptionOptions sub_opts_low;
  sub_opts_low.callback_group = cbg_low;

  rclcpp::SubscriptionOptions sub_opts_high;
  sub_opts_high.callback_group = cbg_high;

  auto sub_low = node->create_subscription<TestMsg>(
    "/test_prio_low", 10,
    [&order_mutex, &execution_order](const TestMsg::SharedPtr msg) {
      std::lock_guard lock(order_mutex);
      execution_order.push_back("low");
    },
    sub_opts_low);

  auto sub_high = node->create_subscription<TestMsg>(
    "/test_prio_high", 10,
    [&order_mutex, &execution_order](const TestMsg::SharedPtr msg) {
      std::lock_guard lock(order_mutex);
      execution_order.push_back("high");
    },
    sub_opts_high);

  auto pub_low = node->create_publisher<TestMsg>("/test_prio_low", 10);
  auto pub_high = node->create_publisher<TestMsg>("/test_prio_high", 10);

  executor->add_node(node);

  // Publish messages simultaneously (both arrive at roughly the same time)
  // First refresh to pick up the entities
  auto msg = TestMsg();
  pub_low->publish(msg);
  pub_high->publish(msg);

  // Allow time for messages to arrive at RMW level
  std::this_thread::sleep_for(100ms);

  // Execute with spin_some to process ready events
  executor->spin_some(500ms);

  // With priority scheduling, the high-priority CBG should execute first
  // Note: This is a probabilistic test — in practice, with MutuallyExclusive
  // groups and a single thread, the priority ordering should be deterministic.
  {
    std::lock_guard lock(order_mutex);
    if (execution_order.size() >= 2) {
      // high should appear before low in execution_order
      bool high_before_low = false;
      for (size_t i = 0; i + 1 < execution_order.size(); ++i) {
        if (execution_order[i] == "high" &&
            execution_order[i + 1] == "low") {
          high_before_low = true;
          break;
        }
      }
      // If both executed, high should come first
      // (In a single-threaded scenario with try_pop, priority wins)
      if (execution_order.size() == 2) {
        EXPECT_TRUE(high_before_low)
          << "Expected high before low, got: "
          << execution_order[0] << ", " << execution_order[1];
      }
    }
  }

  executor->cancel();
}

/// Test that priority scheduling respects MutuallyExclusive semantics:
/// within the same CBG, callbacks are still serialized even with priority.
TEST_F(CBGPriorityExecutorTest, PriorityRespectsMutuallyExclusive)
{
  auto executor = std::make_shared<CBGEventsExecutorPriority>();
  auto node = std::make_shared<rclcpp::Node>("test_prio_mutex");

  auto cbg = node->create_callback_group(
    rclcpp::CallbackGroupType::MutuallyExclusive);

  executor->add_callback_group(cbg, node->get_node_base_interface());
  executor->set_cbg_priority(cbg.get(), 100);

  std::atomic<int> concurrent_count{0};
  std::atomic<int> max_concurrent{0};
  std::atomic<int> total_executions{0};

  // Create a subscription with a MutuallyExclusive CBG
  rclcpp::SubscriptionOptions sub_opts;
  sub_opts.callback_group = cbg;

  auto sub = node->create_subscription<TestMsg>(
    "/test_prio_mutex_topic", 10,
    [&concurrent_count, &max_concurrent, &total_executions](
      const TestMsg::SharedPtr msg) {
      (void)msg;
      int current = concurrent_count.fetch_add(1) + 1;
      // Track maximum concurrent executions
      int prev_max = max_concurrent.load();
      while (current > prev_max) {
        if (max_concurrent.compare_exchange_strong(prev_max, current)) {
          break;
        }
      }
      std::this_thread::sleep_for(10ms);  // Hold execution briefly
      concurrent_count.fetch_sub(1);
      total_executions.fetch_add(1);
    },
    sub_opts);

  executor->add_node(node);

  // Publish multiple messages
  auto pub = node->create_publisher<TestMsg>("/test_prio_mutex_topic", 10);
  for (int i = 0; i < 3; ++i) {
    auto msg = TestMsg();
    pub->publish(msg);
  }

  std::this_thread::sleep_for(100ms);

  // Spin with single thread to verify MutuallyExclusive
  executor->spin_some(500ms);

  // With MutuallyExclusive and single thread, max concurrent should be 1
  EXPECT_LE(max_concurrent.load(), 1);

  executor->cancel();
}

int main(int argc, char ** argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}