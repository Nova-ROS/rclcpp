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

#ifndef RCLCPP__EXECUTORS__CBG__CBG_EVENTS_EXECUTOR_OPTIONS_HPP_
#define RCLCPP__EXECUTORS__CBG__CBG_EVENTS_EXECUTOR_OPTIONS_HPP_

#include <chrono>
#include <cstddef>
#include <memory>
#include <type_traits>

namespace rclcpp::executors::cbg {

/// Options for CBGEventsExecutor, following rclcpp's OptionsWithAllocator pattern.
template<typename AllocatorT = std::allocator<void>>
struct CBGEventsExecutorOptions
{
  static_assert(
    std::is_void_v<typename std::allocator_traits<AllocatorT>::value_type>,
    "AllocatorT value_type must be void (use std::allocator<void>)");

  /// Number of worker threads. 0 = auto-detect (hardware_concurrency, min 2).
  size_t number_of_threads = 0;

  /// Timeout for waiting on events.
  std::chrono::nanoseconds timeout = std::chrono::nanoseconds(-1);

  /// Ring buffer capacity per CBG (must be power of 2).
  size_t events_capacity_per_cbg = 1024;

  /// Custom allocator.
  std::shared_ptr<AllocatorT> allocator = nullptr;

  /// Get allocator (lazily creates default).
  std::shared_ptr<AllocatorT> get_allocator() const
  {
    if (!allocator) {
      if (!allocator_storage_) {
        allocator_storage_ = std::make_shared<AllocatorT>();
      }
      return allocator_storage_;
    }
    return allocator;
  }

private:
  mutable std::shared_ptr<AllocatorT> allocator_storage_;
};

using CBGEventsExecutorOptionsDefault = CBGEventsExecutorOptions<std::allocator<void>>;

}  // namespace rclcpp::executors::cbg

#endif  // RCLCPP__EXECUTORS__CBG__CBG_EVENTS_EXECUTOR_OPTIONS_HPP_
