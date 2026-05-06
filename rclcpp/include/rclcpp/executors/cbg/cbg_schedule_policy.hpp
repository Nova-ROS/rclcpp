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

#ifndef RCLCPP__EXECUTORS__CBG__CBG_SCHEDULE_POLICY_HPP_
#define RCLCPP__EXECUTORS__CBG__CBG_SCHEDULE_POLICY_HPP_

#include <array>
#include <cstddef>

namespace rclcpp::executors::cbg {

/// Non-template base struct for intrusive linked list nodes (defined in cbg_handle.hpp).
/// Forward-declared here so schedule policy traits can use it.
struct CBGHandleNode;

/// FIFO scheduling policy tag
struct FIFOSchedule {};

/// Priority scheduling policy tag
struct PrioritySchedule {};

/// Traits for scheduling policies. Specialize for custom policies.
template<typename Derived>
struct SchedulePolicyTraits;

/// FIFO policy traits: intrusive linked list ready container
template<>
struct SchedulePolicyTraits<FIFOSchedule>
{
  /// Intrusive singly-linked list with tail pointer for O(1) push_back/pop_front.
  /// Operates on CBGHandleNode* (non-template base) for type safety.
  struct ReadyContainer
  {
    CBGHandleNode * head = nullptr;
    CBGHandleNode * tail = nullptr;

    void push_back(CBGHandleNode * h);
    void push_front(CBGHandleNode * h);
    CBGHandleNode * pop_front();
    bool empty() const { return head == nullptr; }
  };
};

/// Priority policy traits: bucketed ready container
template<>
struct SchedulePolicyTraits<PrioritySchedule>
{
  /// Priority metadata stored per CBG
  struct CBGPriorityData
  {
    int priority = 0;
    uint64_t sequence = 0;  // for FIFO ordering within same priority
  };

  /// Multi-bucket queue: each priority level has its own intrusive list.
  /// Constant-time push, amortized constant-time pop (finds highest non-empty bucket).
  struct ReadyContainer
  {
    static constexpr int kMaxPriority = 256;
    static constexpr int kMinPriority = -256;
    static constexpr size_t kNumBuckets =
      static_cast<size_t>(kMaxPriority - kMinPriority + 1);

    struct Bucket
    {
      CBGHandleNode * head = nullptr;
      CBGHandleNode * tail = nullptr;
    };

    std::array<Bucket, kNumBuckets> buckets_{};
    int highest_non_empty_ = kMinPriority - 1;

    void push_back(CBGHandleNode * h, int priority);
    void push_front(CBGHandleNode * h, int priority);
    CBGHandleNode * pop_front();
    bool empty() const { return highest_non_empty_ < kMinPriority; }
  };
};

}  // namespace rclcpp::executors::cbg

#endif  // RCLCPP__EXECUTORS__CBG__CBG_SCHEDULE_POLICY_HPP_