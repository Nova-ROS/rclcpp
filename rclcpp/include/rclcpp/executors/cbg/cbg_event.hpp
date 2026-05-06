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

#ifndef RCLCPP__EXECUTORS__CBG__CBG_EVENT_HPP_
#define RCLCPP__EXECUTORS__CBG__CBG_EVENT_HPP_

#include <cstdint>
#include <type_traits>

#include "rclcpp/experimental/executors/events_executor/events_executor_event_types.hpp"

namespace rclcpp::executors::cbg {

/// Entity key with generation counter to prevent ABA problems.
/// When an entity at the same address is re-registered, its generation increments,
/// ensuring stale events from the old entity are discarded.
struct EntityKey
{
  const void * ptr = nullptr;
  uint32_t generation = 0;

  bool operator<(const EntityKey & other) const
  {
    if (ptr != other.ptr) {return ptr < other.ptr;}
    return generation < other.generation;
  }

  bool operator==(const EntityKey & other) const
  {
    return ptr == other.ptr && generation == other.generation;
  }
};

/// Lightweight event descriptor for CBG-aware execution.
/// Trivially copyable, no shared_ptr/weak_ptr — zero-allocation on the hot path.
struct CBGEvent
{
  const void * entity_key = nullptr;      // rcl handle or Waitable*
  uint32_t entity_generation = 0;         // matches EntityKey::generation at registration time
  void * timer_data = nullptr;            // non-owning, only for TIMER_EVENT
  int waitable_data = -1;                 // only for WAITABLE_EVENT
  rclcpp::experimental::executors::ExecutorEventType type =
    rclcpp::experimental::executors::ExecutorEventType::SUBSCRIPTION_EVENT;
  uint8_t num_events = 1;                 // batch count (max 255)
  rclcpp::CallbackGroup * cbg_ptr = nullptr;  // non-owning, lifetime guaranteed by CBGHandle
};

static_assert(std::is_trivially_copyable_v<CBGEvent>, "CBGEvent must be trivially copyable");
static_assert(sizeof(CBGEvent) <= 56, "CBGEvent should fit in a cache line");

}  // namespace rclcpp::executors::cbg

#endif  // RCLCPP__EXECUTORS__CBG__CBG_EVENT_HPP_
