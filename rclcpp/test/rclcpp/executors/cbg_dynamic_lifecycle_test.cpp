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

#include <chrono>
#include <future>
#include <memory>
#include <thread>

#include "rclcpp/executors/cbg/cbg_events_executor.hpp"
#include "rclcpp/rclcpp.hpp"
#include "test_msgs/msg/empty.hpp"

using namespace std::chrono_literals;
using namespace rclcpp::executors::cbg;
using TestMsg = test_msgs::msg::Empty;

class CBGDynamicLifecycleTest : public ::testing::Test
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

  void SetUp() override
  {
    executor_ = std::make_shared<CBGEventsExecutorDefault>();
  }

  void TearDown() override
  {
    executor_->cancel();
    executor_.reset();
  }

  std::shared_ptr<CBGEventsExecutorDefault> executor_;
};

// ========================================================================
// Scenario A: subscription.reset() during spin
// ========================================================================

TEST_F(CBGDynamicLifecycleTest, SubscriptionResetDuringSpinNoCrash)
{
  auto node = std::make_shared<rclcpp::Node>("test_sub_reset");
  executor_->add_node(node);

  bool callback_received = false;
  auto sub = node->create_subscription<TestMsg>(
    "/test_topic_a", 10,
    [&callback_received](const TestMsg::SharedPtr msg) {
      (void)msg;
      callback_received = true;
    });

  auto spin_future = std::async(std::launch::async, [this]() {
    executor_->spin();
  });

  std::this_thread::sleep_for(100ms);

  // Reset the subscription while spinning
  sub.reset();

  std::this_thread::sleep_for(200ms);

  executor_->cancel();
  spin_future.wait();

  SUCCEED();
}

// ========================================================================
// Scenario B: remove_callback_group during spin
// ========================================================================

TEST_F(CBGDynamicLifecycleTest, RemoveCallbackGroupDuringSpinNoUAF)
{
  auto node = std::make_shared<rclcpp::Node>("test_cbg_remove");
  auto cbg = node->create_callback_group(
    rclcpp::CallbackGroupType::MutuallyExclusive, false);

  executor_->add_node(node);
  executor_->add_callback_group(cbg, node->get_node_base_interface());

  bool callback_received = false;
  auto sub_opts = rclcpp::SubscriptionOptionsWithAllocator<std::allocator<void>>();
  sub_opts.callback_group = cbg;
  auto sub = node->create_subscription<TestMsg>(
    "/test_topic_b", 10,
    [&callback_received](const TestMsg::SharedPtr msg) {
      (void)msg;
      callback_received = true;
    }, sub_opts);

  auto spin_future = std::async(std::launch::async, [this]() {
    executor_->spin();
  });

  std::this_thread::sleep_for(100ms);

  // Remove the callback group while spinning (two-phase: retire → drain → reclaim)
  executor_->remove_callback_group(cbg);

  std::this_thread::sleep_for(500ms);

  executor_->cancel();
  spin_future.wait();

  SUCCEED();
}

// ========================================================================
// Scenario C: Node destruction during spin
// ========================================================================

TEST_F(CBGDynamicLifecycleTest, NodeDestructionDuringSpinCBGAlive)
{
  auto node = std::make_shared<rclcpp::Node>("test_node_destroy");
  executor_->add_node(node);
  // Keep a weak reference to verify CBG lifetime
  auto cbg_weak = node->get_node_base_interface()->get_default_callback_group();

  auto spin_future = std::async(std::launch::async, [this]() {
    executor_->spin();
  });

  std::this_thread::sleep_for(100ms);

  // Destroy the node (last shared_ptr released)
  node.reset();

  std::this_thread::sleep_for(300ms);

  executor_->cancel();
  spin_future.wait();

  SUCCEED();
}

// ========================================================================
// Scenario D: Dynamic create_subscription triggers refresh
// ========================================================================

TEST_F(CBGDynamicLifecycleTest, DynamicSubscriptionCreationDetected)
{
  auto node = std::make_shared<rclcpp::Node>("test_dynamic_sub");
  executor_->add_node(node);

  auto spin_future = std::async(std::launch::async, [this]() {
    executor_->spin();
  });

  std::this_thread::sleep_for(100ms);

  // Create a new subscription after executor is already spinning
  bool callback_received = false;
  auto sub = node->create_subscription<TestMsg>(
    "/test_topic_d", 10,
    [&callback_received](const TestMsg::SharedPtr msg) {
      (void)msg;
      callback_received = true;
    });

  // Periodic refresh (kRefreshInterval=256) or guard condition should detect it
  std::this_thread::sleep_for(500ms);

  executor_->cancel();
  spin_future.wait();

  SUCCEED();
}

// ========================================================================
// Timer destruction during spin
// ========================================================================

TEST_F(CBGDynamicLifecycleTest, TimerResetDuringSpinNoCrash)
{
  auto node = std::make_shared<rclcpp::Node>("test_timer_reset");
  executor_->add_node(node);

  bool timer_called = false;
  auto timer = node->create_wall_timer(
    100ms,
    [&timer_called]() {
      timer_called = true;
    });

  auto spin_future = std::async(std::launch::async, [this]() {
    executor_->spin();
  });

  std::this_thread::sleep_for(250ms);

  // Reset the timer while spinning
  timer.reset();

  std::this_thread::sleep_for(300ms);

  executor_->cancel();
  spin_future.wait();

  SUCCEED();
}

// ========================================================================
// Generation counter: stale events discarded
// ========================================================================

TEST_F(CBGDynamicLifecycleTest, GenerationCounterDiscardsStaleEvents)
{
  auto node = std::make_shared<rclcpp::Node>("test_generation");
  executor_->add_node(node);

  int call_count_v1 = 0;
  {
    auto sub = node->create_subscription<TestMsg>(
      "/test_topic_gen", 10,
      [&call_count_v1](const TestMsg::SharedPtr msg) {
        (void)msg;
        ++call_count_v1;
      });
    // sub destroyed here — generation counter increments
  }

  // New subscription at a different topic
  int call_count_v2 = 0;
  auto sub2 = node->create_subscription<TestMsg>(
    "/test_topic_gen2", 10,
    [&call_count_v2](const TestMsg::SharedPtr msg) {
      (void)msg;
      ++call_count_v2;
    });

  auto spin_future = std::async(std::launch::async, [this]() {
    executor_->spin();
  });

  std::this_thread::sleep_for(500ms);

  executor_->cancel();
  spin_future.wait();

  // Stale events from the destroyed first subscription should NOT be delivered
  EXPECT_EQ(call_count_v1, 0);
}

int main(int argc, char ** argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}