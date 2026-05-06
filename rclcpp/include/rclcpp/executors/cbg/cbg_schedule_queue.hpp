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

#ifndef RCLCPP__EXECUTORS__CBG__CBG_SCHEDULE_QUEUE_HPP_
#define RCLCPP__EXECUTORS__CBG__CBG_SCHEDULE_QUEUE_HPP_

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <type_traits>
#include <utility>

#include "rclcpp/callback_group.hpp"
#include "rclcpp/executors/cbg/cbg_event.hpp"
#include "rclcpp/executors/cbg/cbg_handle.hpp"
#include "rclcpp/executors/cbg/cbg_schedule_policy.hpp"
#include "rclcpp/executors/cbg/detail/alloc_flat_map.hpp"
#include "rclcpp/executors/cbg/detail/alloc_vector.hpp"
#include "rclcpp/executors/cbg/detail/cbg_handle_pool.hpp"

namespace rclcpp::executors::cbg {

/// CBG-aware schedule queue with pluggable scheduling policy.
/// Manages CBGHandle lifecycle (two-phase: register → retire → reclaim).
template<
  typename SchedulePolicy = FIFOSchedule,
  typename AllocatorT = std::allocator<void>>
class CBGScheduleQueue
{
  using Traits = SchedulePolicyTraits<SchedulePolicy>;
  using HandleType = CBGHandle<AllocatorT>;
  using HandlePool = detail::CBGHandlePool<AllocatorT>;

public:
  explicit CBGScheduleQueue(const AllocatorT & alloc = {})
  : alloc_(alloc),
    handle_pool_(16, alloc),
    handles_(alloc),
    graveyard_(alloc)
  {
    handles_.reserve(64);
  }

  ~CBGScheduleQueue()
  {
    for (auto & [key, handle] : handles_) {
      handle_pool_.destroy(handle);
    }
    for (auto * handle : graveyard_) {
      handle_pool_.destroy(handle);
    }
  }

  // --- CBG lifecycle ---

  /// Register a callback group, return its handle.
  HandleType * register_group(
    rclcpp::CallbackGroup::SharedPtr group,
    size_t ring_capacity = 1024)
  {
    auto * handle = handle_pool_.create(group, ring_capacity, alloc_);
    {
      std::lock_guard lock(mutex_);
      handles_.emplace(group.get(), handle);
    }
    return handle;
  }

  /// Two-phase destruction step 1: retire handle (keep alive until drained).
  void retire_group(rclcpp::CallbackGroup * group)
  {
    HandleType * handle = nullptr;
    {
      std::lock_guard lock(mutex_);
      auto * ptr = handles_.find(group);
      if (ptr && *ptr) {
        handle = *ptr;
        handles_.erase(group);
      }
    }
    if (handle) {
      handle->retire();
      {
        std::lock_guard lock(graveyard_mutex_);
        graveyard_.push_back(handle);
      }
    }
  }

  /// Two-phase destruction step 2: reclaim retired handles that are fully drained.
  void reclaim_retired()
  {
    std::lock_guard lock(graveyard_mutex_);
    auto it = std::remove_if(graveyard_.begin(), graveyard_.end(),
      [this](HandleType * h) {
        if (h->is_reclaimable()) {
          handle_pool_.destroy(h);
          return true;
        }
        return false;
      });
    graveyard_.erase(it, graveyard_.end());
  }

  /// Find handle by CBG pointer (active handles only).
  HandleType * find(rclcpp::CallbackGroup * group) const
  {
    std::lock_guard lock(mutex_);
    auto * ptr = handles_.find(group);
    return ptr ? *ptr : nullptr;
  }

  // --- Scheduling operations ---

  /// Notify that a CBG has ready events (called from RMW callback).
  void notify_ready(HandleType * handle)
  {
    {
      std::lock_guard lock(mutex_);
      CBGHandleNode * node = static_cast<CBGHandleNode *>(handle);
      if (!node->in_ready_queue) {
        node->in_ready_queue = true;
        if constexpr (std::is_same_v<SchedulePolicy, FIFOSchedule>) {
          ready_.push_back(node);
        } else {
          ready_.push_back(node, get_priority(handle));
        }
      }
    }
    cv_.notify_one();
  }

  /// Blocking wait for next ready event.
  bool wait_and_pop(CBGEvent & out, std::chrono::nanoseconds timeout)
  {
    std::unique_lock lock(mutex_);
    while (!try_pop_impl(out)) {
      if (!spinning_.load(std::memory_order_relaxed)) {return false;}
      if (cv_.wait_for(lock, timeout) == std::cv_status::timeout) {
        return try_pop_impl(out);
      }
    }
    return true;
  }

  /// Non-blocking try to get next event.
  bool try_pop(CBGEvent & out)
  {
    std::lock_guard lock(mutex_);
    return try_pop_impl(out);
  }

  /// Mark entity execution complete; release CBG and possibly re-queue.
  void mark_executed(HandleType * handle, bool has_more)
  {
    handle->release();
    if (has_more) {
      notify_ready(handle);
    }
  }

  // --- Priority control (only meaningful for PrioritySchedule) ---

  /// Set the priority for a CBG handle.
  /// Only meaningful when SchedulePolicy is PrioritySchedule; no-op for FIFO.
  /// Priority range: -256 to 256. Higher priority = executed first.
  void
  set_priority(HandleType * handle, int priority)
  {
    if constexpr (std::is_same_v<SchedulePolicy, PrioritySchedule>) {
      using ReadyCont = typename Traits::ReadyContainer;
      if (priority < ReadyCont::kMinPriority ||
          priority > ReadyCont::kMaxPriority) {
        return;  // Out of range, silently ignored
      }
      std::lock_guard lock(mutex_);
      auto * existing = priority_data_.find(handle);
      if (existing) {
        existing->priority = priority;
      } else {
        PriorityData pd;
        pd.priority = priority;
        pd.sequence = 0;
        priority_data_.emplace(handle, pd);
      }
    }
    (void)handle;
    (void)priority;
  }

  /// Get the priority for a CBG (by CallbackGroup pointer).
  /// Returns 0 for FIFOSchedule or if the CBG has no explicit priority set.
  int
  get_priority(rclcpp::CallbackGroup * cbg) const
  {
    if constexpr (std::is_same_v<SchedulePolicy, PrioritySchedule>) {
      auto * handle = find(cbg);
      if (!handle) {return 0;}
      std::lock_guard lock(mutex_);
      auto * pd = priority_data_.find(handle);
      return pd ? pd->priority : 0;
    }
    (void)cbg;
    return 0;
  }

  // --- Control ---

  /// Check if there are any ready events (approximate, for spin_some).
  bool has_ready_events() const
  {
    std::lock_guard lock(mutex_);
    return !ready_.empty();
  }

  void start() { spinning_.store(true); }
  void stop()
  {
    spinning_.store(false);
    cv_.notify_all();
  }

  /// Wake up all threads waiting on the condition variable.
  /// Used by the executor to signal refresh needs without enqueuing an event.
  void wake()
  {
    cv_.notify_all();
  }

private:
  /// Convert CBGHandleNode* to HandleType* (safe downcast via inheritance).
  static HandleType * node_to_handle(CBGHandleNode * node)
  {
    return static_cast<HandleType *>(node);
  }

  /// Internal: try to get a ready event from the ready queue.
  bool try_pop_impl(CBGEvent & out)
  {
    using NodeReady = typename Traits::ReadyContainer;

    // Skipped handles (MutuallyExclusive busy) are saved and put back.
    NodeReady skipped;

    while (!ready_.empty()) {
      CBGHandleNode * node = ready_.pop_front();
      HandleType * handle = node_to_handle(node);

      node->in_ready_queue = false;

      if (handle->try_acquire()) {
        CBGEvent event;
        if (handle->dequeue(event)) {
          if (handle->has_ready()) {
            node->in_ready_queue = true;
            if constexpr (std::is_same_v<SchedulePolicy, FIFOSchedule>) {
              ready_.push_back(node);
            } else {
              ready_.push_back(node, get_priority(handle));
            }
          }
          // Put skipped back at front
          while (!skipped.empty()) {
            CBGHandleNode * s = skipped.pop_front();
            if constexpr (std::is_same_v<SchedulePolicy, FIFOSchedule>) {
              ready_.push_front(s);
            } else {
              ready_.push_front(s, get_priority(node_to_handle(s)));
            }
          }
          out = event;
          return true;
        } else {
          // Spurious ready (another thread consumed it first)
          handle->release();
        }
      } else {
        // CBG is busy (MutuallyExclusive), skip
        if constexpr (std::is_same_v<SchedulePolicy, FIFOSchedule>) {
          skipped.push_back(node);
        } else {
          skipped.push_back(node, get_priority(handle));
        }
      }
    }

    // Put skipped back
    while (!skipped.empty()) {
      CBGHandleNode * s = skipped.pop_front();
      if constexpr (std::is_same_v<SchedulePolicy, FIFOSchedule>) {
        ready_.push_back(s);
      } else {
        ready_.push_back(s, get_priority(node_to_handle(s)));
      }
    }
    return false;
  }

  int get_priority(const HandleType * h) const
  {
    if constexpr (std::is_same_v<SchedulePolicy, PrioritySchedule>) {
      auto * pd = priority_data_.find(const_cast<HandleType *>(h));
      return pd ? pd->priority : 0;
    }
    return 0;
  }

  AllocatorT alloc_;
  HandlePool handle_pool_;

  mutable std::mutex mutex_;
  std::condition_variable cv_;
  std::atomic<bool> spinning_{false};

  // Active handles: CBG* → Handle*
  detail::AllocFlatMap<rclcpp::CallbackGroup *, HandleType *, AllocatorT> handles_;

  // Ready queue (intrusive linked list via CBGHandleNode)
  typename Traits::ReadyContainer ready_;

  // Graveyard for retired handles
  std::mutex graveyard_mutex_;
  detail::AllocVector<HandleType *, AllocatorT> graveyard_;

  // Priority data (only used with PrioritySchedule)
  using PriorityData = typename SchedulePolicyTraits<PrioritySchedule>::CBGPriorityData;
  detail::AllocFlatMap<HandleType *, PriorityData, AllocatorT> priority_data_{alloc_};
};

// --- ReadyContainer method implementations ---
// All operate on CBGHandleNode* (non-template base) for type safety.

// FIFO ReadyContainer
inline void SchedulePolicyTraits<FIFOSchedule>::ReadyContainer::push_back(
  CBGHandleNode * h)
{
  h->next_ready = nullptr;
  if (tail) {
    tail->next_ready = h;
  } else {
    head = h;
  }
  tail = h;
}

inline void SchedulePolicyTraits<FIFOSchedule>::ReadyContainer::push_front(
  CBGHandleNode * h)
{
  h->next_ready = head;
  head = h;
  if (!tail) {tail = h;}
}

inline CBGHandleNode * SchedulePolicyTraits<FIFOSchedule>::ReadyContainer::pop_front()
{
  if (!head) {return nullptr;}
  CBGHandleNode * h = head;
  head = head->next_ready;
  if (!head) {tail = nullptr;}
  h->next_ready = nullptr;
  return h;
}

// Priority ReadyContainer
inline void SchedulePolicyTraits<PrioritySchedule>::ReadyContainer::push_back(
  CBGHandleNode * h, int priority)
{
  auto & bucket = buckets_[static_cast<size_t>(priority - kMinPriority)];
  h->next_ready = nullptr;
  if (bucket.tail) {
    bucket.tail->next_ready = h;
  } else {
    bucket.head = h;
  }
  bucket.tail = h;
  if (priority > highest_non_empty_) {
    highest_non_empty_ = priority;
  }
}

inline void SchedulePolicyTraits<PrioritySchedule>::ReadyContainer::push_front(
  CBGHandleNode * h, int priority)
{
  auto & bucket = buckets_[static_cast<size_t>(priority - kMinPriority)];
  h->next_ready = bucket.head;
  bucket.head = h;
  if (!bucket.tail) {bucket.tail = h;}
  if (priority > highest_non_empty_) {
    highest_non_empty_ = priority;
  }
}

inline CBGHandleNode * SchedulePolicyTraits<PrioritySchedule>::ReadyContainer::pop_front()
{
  while (highest_non_empty_ >= kMinPriority) {
    auto & bucket = buckets_[static_cast<size_t>(highest_non_empty_ - kMinPriority)];
    if (bucket.head) {
      CBGHandleNode * h = bucket.head;
      bucket.head = h->next_ready;
      if (!bucket.head) {bucket.tail = nullptr;}
      h->next_ready = nullptr;
      return h;
    }
    --highest_non_empty_;
  }
  return nullptr;
}

}  // namespace rclcpp::executors::cbg

#endif  // RCLCPP__EXECUTORS__CBG__CBG_SCHEDULE_QUEUE_HPP_