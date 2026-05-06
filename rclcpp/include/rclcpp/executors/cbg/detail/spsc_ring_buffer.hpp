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

#ifndef RCLCPP__EXECUTORS__CBG__DETAIL__SPSC_RING_BUFFER_HPP_
#define RCLCPP__EXECUTORS__CBG__DETAIL__SPSC_RING_BUFFER_HPP_

#include <atomic>
#include <cstddef>
#include <memory>
#include <new>
#include <optional>
#include <type_traits>

namespace rclcpp::executors::cbg::detail {

/// Bounded SPSC (single-producer single-consumer) ring buffer.
/// Power-of-2 capacity with mask-based indexing (no modulo division).
/// head_/tail_ on separate cache lines to avoid false sharing.
/// Allocator-aware: uses allocator_traits for construct/destroy.
template<
  typename T,
  typename AllocatorT = std::allocator<void>,
  size_t Capacity = 1024>
class SPSCRingBuffer
{
  static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be power of 2");
  static constexpr size_t kMask = Capacity - 1;

  using AllocTraits = std::allocator_traits<AllocatorT>;
  using BufferAlloc = typename AllocTraits::template rebind_alloc<T>;

public:
  explicit SPSCRingBuffer(const AllocatorT & alloc = {})
  : alloc_(alloc)
  {
    buffer_ = std::allocator_traits<BufferAlloc>::allocate(alloc_, Capacity);
  }

  ~SPSCRingBuffer()
  {
    // Destroy remaining elements
    const size_t tail = tail_.load(std::memory_order_relaxed);
    const size_t head = head_.load(std::memory_order_relaxed);
    for (size_t i = tail; i != head; i = (i + 1) & kMask) {
      std::allocator_traits<BufferAlloc>::destroy(alloc_, &buffer_[i]);
    }
    std::allocator_traits<BufferAlloc>::deallocate(alloc_, buffer_, Capacity);
  }

  SPSCRingBuffer(const SPSCRingBuffer &) = delete;
  SPSCRingBuffer & operator=(const SPSCRingBuffer &) = delete;

  /// Producer: push an item. Returns false if queue was full (overwrites oldest).
  bool push(T && item)
  {
    const size_t head = head_.load(std::memory_order_relaxed);
    const size_t next = (head + 1) & kMask;

    if (next == tail_.load(std::memory_order_acquire)) {
      // Queue full — overwrite oldest entry
      auto & slot = buffer_[tail_.load(std::memory_order_relaxed)];
      std::allocator_traits<BufferAlloc>::destroy(alloc_, &slot);
      tail_.store((tail_.load(std::memory_order_relaxed) + 1) & kMask,
                   std::memory_order_release);
      std::allocator_traits<BufferAlloc>::construct(alloc_, &buffer_[head], std::move(item));
      head_.store(next, std::memory_order_release);
      return false;
    }

    std::allocator_traits<BufferAlloc>::construct(alloc_, &buffer_[head], std::move(item));
    head_.store(next, std::memory_order_release);
    return true;
  }

  /// Consumer: pop an item into output param. Returns false if empty.
  bool pop(T & out)
  {
    const size_t tail = tail_.load(std::memory_order_relaxed);
    if (tail == head_.load(std::memory_order_acquire)) {
      return false;
    }
    out = std::move(buffer_[tail]);
    std::allocator_traits<BufferAlloc>::destroy(alloc_, &buffer_[tail]);
    tail_.store((tail + 1) & kMask, std::memory_order_release);
    return true;
  }

  /// Approximate non-empty check (consumer side).
  bool not_empty() const
  {
    return tail_.load(std::memory_order_relaxed) !=
           head_.load(std::memory_order_acquire);
  }

private:
  alignas(64) std::atomic<size_t> head_{0};
  alignas(64) std::atomic<size_t> tail_{0};
  T * buffer_ = nullptr;
  BufferAlloc alloc_;
};

}  // namespace rclcpp::executors::cbg::detail

#endif  // RCLCPP__EXECUTORS__CBG__DETAIL__SPSC_RING_BUFFER_HPP_
