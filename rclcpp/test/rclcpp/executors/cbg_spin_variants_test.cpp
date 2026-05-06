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
#include <memory>
#include <thread>

#include "rclcpp/executors/cbg/cbg_events_executor.hpp"
#include "rclcpp/rclcpp.hpp"
#include "test_msgs/msg/empty.hpp"

using namespace std::chrono_literals;
using namespace rclcpp::executors::cbg;
using TestMsg = test_msgs::msg::Empty;

class CBGSpinVariantsTest : public ::testing::Test
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

// ========================================================================
// spin_some: executes currently ready events and returns
// ========================================================================

TEST_F(CBGSpinVariantsTest, SpinSomeExecutesAvailableWork)
{
  auto executor = std::make_shared<CBGEventsExecutorDefault>();
  auto node = std::make_shared<rclcpp::Node>("test_spin_some");

  int count = 0;
  auto sub = node->create_subscription<TestMsg>(
    "/test_spin_some_topic", 10,
    [&count](const TestMsg::SharedPtr msg) {
      (void)msg;
      ++count;
    });

  executor->add_node(node);

  // Create a publisher on the same topic
  auto pub = node->create_publisher<TestMsg>("/test_spin_some_topic", 10);

  // Publish a message
  auto msg = TestMsg();
  pub->publish(msg);

  // Allow some time for the message to arrive
  std::this_thread::sleep_for(100ms);

  // spin_some should execute the available callback
  executor->spin_some(100ms);

  EXPECT_GT(count, 0);

  executor->cancel();
}

// ========================================================================
// spin_all: exhaustively executes until timeout
// ========================================================================

TEST_F(CBGSpinVariantsTest, SpinAllExecutesUntilTimeout)
{
  auto executor = std::make_shared<CBGEventsExecutorDefault>();
  auto node = std::make_shared<rclcpp::Node>("test_spin_all");

  int count = 0;
  auto sub = node->create_subscription<TestMsg>(
    "/test_spin_all_topic", 10,
    [&count](const TestMsg::SharedPtr msg) {
      (void)msg;
      ++count;
    });

  executor->add_node(node);

  auto pub = node->create_publisher<TestMsg>("/test_spin_all_topic", 10);

  // Publish multiple messages
  for (int i = 0; i < 5; ++i) {
    auto msg = TestMsg();
    pub->publish(msg);
  }

  std::this_thread::sleep_for(100ms);

  // spin_all should exhaustively process all available messages
  executor->spin_all(200ms);

  // All messages should have been processed
  EXPECT_EQ(count, 5);

  executor->cancel();
}

// ========================================================================
// spin_once: waits for one event
// ========================================================================

TEST_F(CBGSpinVariantsTest, SpinOnceWaitsForOneEvent)
{
  auto executor = std::make_shared<CBGEventsExecutorDefault>();
  auto node = std::make_shared<rclcpp::Node>("test_spin_once");

  bool received = false;
  auto sub = node->create_subscription<TestMsg>(
    "/test_spin_once_topic", 10,
    [&received](const TestMsg::SharedPtr msg) {
      (void)msg;
      received = true;
    });

  executor->add_node(node);

  // Publish after a short delay
  auto pub = node->create_publisher<TestMsg>("/test_spin_once_topic", 10);
  std::thread publisher_thread([&pub]() {
    std::this_thread::sleep_for(50ms);
    auto msg = TestMsg();
    pub->publish(msg);
  });

  // spin_once should wait and then execute the callback
  executor->spin_once(500ms);

  publisher_thread.join();

  EXPECT_TRUE(received);

  executor->cancel();
}

// ========================================================================
// Timer with spin_once: timer fires before timeout
// ========================================================================

TEST_F(CBGSpinVariantsTest, SpinOnceTimerExecuted)
{
  auto executor = std::make_shared<CBGEventsExecutorDefault>();
  auto node = std::make_shared<rclcpp::Node>("test_spin_once_timer");

  bool timer_called = false;
  auto timer = node->create_wall_timer(
    50ms,
    [&timer_called]() {
      timer_called = true;
    });

  executor->add_node(node);

  // spin_once should pick up the timer
  executor->spin_once(200ms);

  EXPECT_TRUE(timer_called);

  executor->cancel();
}

// ========================================================================
// cancel stops spin
// ========================================================================

TEST_F(CBGSpinVariantsTest, CancelStopsSpin)
{
  auto executor = std::make_shared<CBGEventsExecutorDefault>();
  auto node = std::make_shared<rclcpp::Node>("test_cancel");

  executor->add_node(node);

  // Start spin in background
  auto spin_future = std::async(std::launch::async, [&executor]() {
    executor->spin();
  });

  std::this_thread::sleep_for(100ms);

  // Cancel should stop the spin
  executor->cancel();

  // spin_future should complete within a reasonable time
  auto status = spin_future.wait_for(2s);
  EXPECT_EQ(status, std::future_status::ready);
}

int main(int argc, char ** argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}