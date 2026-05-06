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

#ifndef RCLCPP__EXECUTORS__CBG__CBG_HANDLE_HPP_
#define RCLCPP__EXECUTORS__CBG__CBG_HANDLE_HPP_

#include <atomic>
#include <cstddef>
#include <memory>

#include "rclcpp/callback_group.hpp"
#include "rclcpp/executors/cbg/cbg_event.hpp"
#include "rclcpp/executors/cbg/detail/spsc_ring_buffer.hpp"

namespace rclcpp::executors::cbg {

/// Non-template base struct for intrusive linked list nodes.
/// All CBGHandle<AllocatorT> inherit from this, so schedule policy
/// ReadyContainer can operate on CBGHandleNode* without template
/// proliferation or reinterpret_cast.
struct CBGHandleNode
{
  CBGHandleNode * next_ready = nullptr;
  bool in_ready_queue = false;
};

/// Per-CallbackGroup handle for the scheduler.
/// Owns an SPSC ring buffer for events and provides mutual exclusion gating.
/// Holds CallbackGroup::SharedPtr to prevent premature CBG destruction.
template<typename AllocatorT = std::allocator<void>>
class CBGHandle : public CBGHandleNode
{
  using RingBuffer = detail::SPSCRingBuffer<CBGEvent, AllocatorT>;

public:
  CBGHandle(
    rclcpp::CallbackGroup::SharedPtr group,
    size_t ring_capacity = 1024,
    const AllocatorT & alloc = {})
  : group_shared_(group),
    group_raw_(group.get()),
    group_type_(group->type()),
    ring_(alloc)
  {}

  // --- Event queue operations ---

  /// Push event into ring buffer (called from RMW callback thread).
  /// Returns false if handle is retired or queue was full (overwrites oldest).
  bool enqueue(CBGEvent event)
  {
    if (retired_.load(std::memory_order_acquire)) {
      return false;
    }
    return ring_.push(std::move(event));
  }

  /// Pop event from ring buffer (called from worker thread).
  bool dequeue(CBGEvent & out)
  {
    return ring_.pop(out);
  }

  /// Check if there are ready events (approximate, consumer side).
  bool has_ready() const { return ring_.not_empty(); }

  // --- Mutual exclusion control ---

  /// Try to acquire execution right for this CBG.
  /// MutuallyExclusive: CAS false->true. Reentrant: always true.
  bool try_acquire()
  {
    if (retired_.load(std::memory_order_acquire)) {return false;}
    if (group_type_ == CallbackGroupType::Reentrant) {return true;}
    bool expected = false;
    return executing_.compare_exchange_strong(
      expected, true, std::memory_order_acq_rel);
  }

  /// Release execution right.
  void release()
  {
    if (group_type_ == CallbackGroupType::MutuallyExclusive) {
      executing_.store(false, std::memory_order_release);
    }
  }

  // --- Retirement (two-phase destruction) ---

  /// Mark as retired: no new events accepted, existing events will be drained.
  void retire()
  {
    retired_.store(true, std::memory_order_release);
  }

  bool is_retired() const
  {
    return retired_.load(std::memory_order_acquire);
  }

  /// Check if safe to destroy: retired + no pending events + not executing.
  bool is_reclaimable() const
  {
    return is_retired() && !has_ready() && !is_executing();
  }

  bool is_executing() const
  {
    if (group_type_ == CallbackGroupType::Reentrant) {return false;}
    return executing_.load(std::memory_order_acquire);
  }

  // --- Metadata ---

  rclcpp::CallbackGroup::SharedPtr callback_group_shared() const { return group_shared_; }
  rclcpp::CallbackGroup * callback_group() const { return group_raw_; }
  CallbackGroupType type() const { return group_type_; }

private:
  rclcpp::CallbackGroup::SharedPtr group_shared_;   // owning: prevents CBG destruction
  rclcpp::CallbackGroup * group_raw_;               // non-owning: fast access
  CallbackGroupType group_type_;
  RingBuffer ring_;
  alignas(64) std::atomic<bool> executing_{false};
  alignas(64) std::atomic<bool> retired_{false};
};

}  // namespace rclcpp::executors::cbg

#endif  // RCLCPP__EXECUTORS__CBG__CBG_HANDLE_HPP_
