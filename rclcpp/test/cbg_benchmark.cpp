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

#include <benchmark/benchmark.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "rclcpp/executors/cbg/cbg_events_executor.hpp"
#include "rclcpp/executors/multi_threaded_executor.hpp"
#include "rclcpp/executors/single_threaded_executor.hpp"
#include "rclcpp/rclcpp.hpp"
#include "std_msgs/msg/string.hpp"

using namespace std::chrono_literals;
using namespace rclcpp::executors::cbg;
using StringMsg = std_msgs::msg::String;
using MTE = rclcpp::executors::MultiThreadedExecutor;
using STE = rclcpp::executors::SingleThreadedExecutor;

// ============================================================================
// Helpers
// ============================================================================

struct LatencyResult
{
  double p50_us;
  double p99_us;
  double mean_us;
};

/// Measure latency for CBGEventsExecutor.
LatencyResult measure_cbg_latency(size_t num_messages, size_t num_threads = 2)
{
  rclcpp::init(0, nullptr);

  CBGEventsExecutorOptions<> opts;
  opts.number_of_threads = num_threads;
  auto executor = std::make_shared<CBGEventsExecutorDefault>(opts);

  auto node = std::make_shared<rclcpp::Node>("bench_latency");

  std::vector<double> latencies;
  std::mutex lat_mutex;
  std::atomic<size_t> received{0};
  auto t0 = std::chrono::steady_clock::now();

  auto sub = node->create_subscription<StringMsg>(
    "/bench_latency", 10,
    [&latencies, &lat_mutex, &received, &t0](const StringMsg::SharedPtr msg) {
      (void)msg;
      auto end = std::chrono::steady_clock::now();
      double us = std::chrono::duration<double, std::micro>(end - t0).count();
      {
        std::lock_guard lock(lat_mutex);
        latencies.push_back(us);
      }
      received.fetch_add(1);
    });

  executor->add_node(node);
  auto pub = node->create_publisher<StringMsg>("/bench_latency", 10);

  auto spin_future = std::async(std::launch::async, [&executor]() {
    executor->spin();
  });

  std::this_thread::sleep_for(100ms);

  for (size_t i = 0; i < num_messages; ++i) {
    auto msg = StringMsg();
    msg.data = "bench_" + std::to_string(i);
    pub->publish(msg);
    std::this_thread::sleep_for(100us);
  }

  auto deadline = std::chrono::steady_clock::now() + 5s;
  while (received.load() < num_messages && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }

  executor->cancel();
  spin_future.wait_for(2s);
  rclcpp::shutdown();

  LatencyResult result{0, 0, 0};
  if (latencies.empty()) {return result;}

  std::sort(latencies.begin(), latencies.end());
  result.p50_us = latencies[latencies.size() / 2];
  result.p99_us = latencies[static_cast<size_t>(latencies.size() * 0.99)];
  double sum = 0;
  for (auto l : latencies) {sum += l;}
  result.mean_us = sum / latencies.size();
  return result;
}

/// Measure latency for MultiThreadedExecutor.
LatencyResult measure_mte_latency(size_t num_messages)
{
  rclcpp::init(0, nullptr);

  auto executor = std::make_shared<MTE>();

  auto node = std::make_shared<rclcpp::Node>("bench_latency_mte");
  std::vector<double> latencies;
  std::mutex lat_mutex;
  std::atomic<size_t> received{0};
  auto t0 = std::chrono::steady_clock::now();

  auto sub = node->create_subscription<StringMsg>(
    "/bench_latency_mte", 10,
    [&latencies, &lat_mutex, &received, &t0](const StringMsg::SharedPtr msg) {
      (void)msg;
      auto end = std::chrono::steady_clock::now();
      double us = std::chrono::duration<double, std::micro>(end - t0).count();
      {
        std::lock_guard lock(lat_mutex);
        latencies.push_back(us);
      }
      received.fetch_add(1);
    });

  executor->add_node(node);
  auto pub = node->create_publisher<StringMsg>("/bench_latency_mte", 10);

  auto spin_future = std::async(std::launch::async, [&executor]() {
    executor->spin();
  });

  std::this_thread::sleep_for(100ms);

  for (size_t i = 0; i < num_messages; ++i) {
    auto msg = StringMsg();
    msg.data = "bench_" + std::to_string(i);
    pub->publish(msg);
    std::this_thread::sleep_for(100us);
  }

  auto deadline = std::chrono::steady_clock::now() + 5s;
  while (received.load() < num_messages && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }

  executor->cancel();
  spin_future.wait_for(2s);
  rclcpp::shutdown();

  LatencyResult result{0, 0, 0};
  if (latencies.empty()) {return result;}

  std::sort(latencies.begin(), latencies.end());
  result.p50_us = latencies[latencies.size() / 2];
  result.p99_us = latencies[static_cast<size_t>(latencies.size() * 0.99)];
  double sum = 0;
  for (auto l : latencies) {sum += l;}
  result.mean_us = sum / latencies.size();
  return result;
}

/// Measure throughput for CBGEventsExecutor.
double measure_cbg_throughput(size_t duration_ms = 1000)
{
  rclcpp::init(0, nullptr);

  CBGEventsExecutorOptions<> opts;
  opts.number_of_threads = 2;
  auto executor = std::make_shared<CBGEventsExecutorDefault>(opts);

  auto node = std::make_shared<rclcpp::Node>("bench_throughput");
  std::atomic<size_t> count{0};

  auto sub = node->create_subscription<StringMsg>(
    "/bench_throughput", 10,
    [&count](const StringMsg::SharedPtr msg) {
      (void)msg;
      count.fetch_add(1, std::memory_order_relaxed);
    });

  executor->add_node(node);
  auto pub = node->create_publisher<StringMsg>("/bench_throughput", 10);

  auto spin_future = std::async(std::launch::async, [&executor]() {
    executor->spin();
  });

  std::this_thread::sleep_for(100ms);

  auto start = std::chrono::steady_clock::now();
  auto deadline = start + std::chrono::milliseconds(duration_ms);

  while (std::chrono::steady_clock::now() < deadline) {
    auto msg = StringMsg();
    msg.data = "t";
    pub->publish(msg);
  }

  std::this_thread::sleep_for(500ms);

  executor->cancel();
  spin_future.wait_for(2s);

  size_t total = count.load();
  rclcpp::shutdown();
  return total / (duration_ms / 1000.0);
}

/// Measure throughput for MultiThreadedExecutor.
double measure_mte_throughput(size_t duration_ms = 1000)
{
  rclcpp::init(0, nullptr);

  auto executor = std::make_shared<MTE>();

  auto node = std::make_shared<rclcpp::Node>("bench_throughput_mte");
  std::atomic<size_t> count{0};

  auto sub = node->create_subscription<StringMsg>(
    "/bench_throughput_mte", 10,
    [&count](const StringMsg::SharedPtr msg) {
      (void)msg;
      count.fetch_add(1, std::memory_order_relaxed);
    });

  executor->add_node(node);
  auto pub = node->create_publisher<StringMsg>("/bench_throughput_mte", 10);

  auto spin_future = std::async(std::launch::async, [&executor]() {
    executor->spin();
  });

  std::this_thread::sleep_for(100ms);

  auto start = std::chrono::steady_clock::now();
  auto deadline = start + std::chrono::milliseconds(duration_ms);

  while (std::chrono::steady_clock::now() < deadline) {
    auto msg = StringMsg();
    msg.data = "t";
    pub->publish(msg);
  }

  std::this_thread::sleep_for(500ms);

  executor->cancel();
  spin_future.wait_for(2s);

  size_t total = count.load();
  rclcpp::shutdown();
  return total / (duration_ms / 1000.0);
}

// ============================================================================
// Latency benchmarks
// ============================================================================

static void BM_CBGEventsExecutor_Latency(benchmark::State & state)
{
  for (auto _ : state) {
    auto result = measure_cbg_latency(100);
    state.counters["p50_us"] = result.p50_us;
    state.counters["p99_us"] = result.p99_us;
    state.counters["mean_us"] = result.mean_us;
  }
}
BENCHMARK(BM_CBGEventsExecutor_Latency)->Iterations(3);

static void BM_MultiThreadedExecutor_Latency(benchmark::State & state)
{
  for (auto _ : state) {
    auto result = measure_mte_latency(100);
    state.counters["p50_us"] = result.p50_us;
    state.counters["p99_us"] = result.p99_us;
    state.counters["mean_us"] = result.mean_us;
  }
}
BENCHMARK(BM_MultiThreadedExecutor_Latency)->Iterations(3);

// ============================================================================
// Throughput benchmarks
// ============================================================================

static void BM_CBGEventsExecutor_Throughput(benchmark::State & state)
{
  for (auto _ : state) {
    double msg_per_sec = measure_cbg_throughput(1000);
    state.counters["msg_per_sec"] = msg_per_sec;
  }
}
BENCHMARK(BM_CBGEventsExecutor_Throughput)->Iterations(3);

static void BM_MultiThreadedExecutor_Throughput(benchmark::State & state)
{
  for (auto _ : state) {
    double msg_per_sec = measure_mte_throughput(1000);
    state.counters["msg_per_sec"] = msg_per_sec;
  }
}
BENCHMARK(BM_MultiThreadedExecutor_Throughput)->Iterations(3);

// ============================================================================
// CBG scheduling overhead benchmark (no ROS middleware)
// ============================================================================

static void BM_CBGScheduleQueue_RoundTrip(benchmark::State & state)
{
  using Queue = CBGScheduleQueue<FIFOSchedule, std::allocator<void>>;

  rclcpp::init(0, nullptr);
  auto node = std::make_shared<rclcpp::Node>("bench_queue");
  auto cbg = node->create_callback_group(rclcpp::CallbackGroupType::MutuallyExclusive);

  Queue queue;
  auto * handle = queue.register_group(cbg, 64);
  queue.start();

  for (auto _ : state) {
    CBGEvent event;
    event.entity_key = nullptr;
    event.type = rclcpp::experimental::executors::ExecutorEventType::SUBSCRIPTION_EVENT;
    event.num_events = 1;
    event.cbg_ptr = cbg.get();

    handle->enqueue(std::move(event));
    queue.notify_ready(handle);

    CBGEvent out;
    if (queue.try_pop(out)) {
      bool has_more = handle->has_ready();
      queue.mark_executed(handle, has_more);
    }
  }

  queue.stop();
  rclcpp::shutdown();
}
BENCHMARK(BM_CBGScheduleQueue_RoundTrip);

// ============================================================================
// Priority scheduling overhead
// ============================================================================

static void BM_CBGScheduleQueue_PrioritySetGet(benchmark::State & state)
{
  using PrioQueue = CBGScheduleQueue<PrioritySchedule, std::allocator<void>>;

  rclcpp::init(0, nullptr);
  auto node = std::make_shared<rclcpp::Node>("bench_prio");
  auto cbg = node->create_callback_group(rclcpp::CallbackGroupType::MutuallyExclusive);

  PrioQueue queue;
  auto * handle = queue.register_group(cbg, 64);

  for (auto _ : state) {
    queue.set_priority(handle, 100);
    int p = queue.get_priority(cbg.get());
    benchmark::DoNotOptimize(p);
  }

  rclcpp::shutdown();
}
BENCHMARK(BM_CBGScheduleQueue_PrioritySetGet);

BENCHMARK_MAIN();
