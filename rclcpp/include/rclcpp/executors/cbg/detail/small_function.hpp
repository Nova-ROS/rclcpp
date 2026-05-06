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

#ifndef RCLCPP__EXECUTORS__CBG__DETAIL__SMALL_FUNCTION_HPP_
#define RCLCPP__EXECUTORS__CBG__DETAIL__SMALL_FUNCTION_HPP_

#include <cstddef>
#include <functional>
#include <new>
#include <type_traits>
#include <utility>

namespace rclcpp::executors::cbg::detail {

/// Type-erased callable with small-buffer optimization (SBO).
/// Guarantees no heap allocation when sizeof(Callable) <= BufferSize.
/// Callable must be nothrow move-constructible.
template<typename Signature, size_t BufferSize = 48, size_t Align = alignof(std::max_align_t)>
class small_function;

template<typename Ret, typename... Args, size_t BufferSize, size_t Align>
class small_function<Ret(Args...), BufferSize, Align>
{
  static constexpr size_t kBufSize = BufferSize;
  alignas(Align) std::byte buffer_[kBufSize];

  using InvokeFn = Ret (*)(const std::byte *, Args...);
  using DestroyFn = void (*)(std::byte *);
  using MoveFn = void (*)(std::byte * dst, std::byte * src);

  InvokeFn invoke_ = nullptr;
  DestroyFn destroy_ = nullptr;
  MoveFn move_ = nullptr;

  template<typename Callable>
  static Ret invoke_impl(const std::byte * buf, Args... args)
  {
    return (*reinterpret_cast<const Callable *>(buf))(std::forward<Args>(args)...);
  }

  template<typename Callable>
  static void destroy_impl(std::byte * buf)
  {
    reinterpret_cast<Callable *>(buf)->~Callable();
  }

  template<typename Callable>
  static void move_impl(std::byte * dst, std::byte * src)
  {
    new (dst) Callable(std::move(*reinterpret_cast<Callable *>(src)));
    reinterpret_cast<Callable *>(src)->~Callable();
  }

public:
  small_function() = default;
  small_function(std::nullptr_t) : small_function() {}

  template<
    typename Callable,
    typename Decayed = std::decay_t<Callable>,
    std::enable_if_t<
      !std::is_same_v<Decayed, small_function> &&
      sizeof(Decayed) <= kBufSize &&
      alignof(Decayed) <= Align &&
      std::is_nothrow_move_constructible_v<Decayed>, int> = 0>
  small_function(Callable && callable)  // NOLINT(bugprone-forwarding-reference-overload)
  {
    static_assert(sizeof(Decayed) <= kBufSize, "Callable too large for small_function buffer");
    static_assert(alignof(Decayed) <= Align, "Callable alignment too large for small_function");
    new (buffer_) Decayed(std::forward<Callable>(callable));
    invoke_ = &invoke_impl<Decayed>;
    destroy_ = &destroy_impl<Decayed>;
    move_ = &move_impl<Decayed>;
  }

  small_function(const small_function &) = delete;
  small_function & operator=(const small_function &) = delete;

  small_function(small_function && other) noexcept
  : invoke_(other.invoke_), destroy_(other.destroy_), move_(other.move_)
  {
    if (move_) {
      move_(buffer_, other.buffer_);
    }
    other.invoke_ = nullptr;
    other.destroy_ = nullptr;
    other.move_ = nullptr;
  }

  small_function & operator=(small_function && other) noexcept
  {
    if (this != &other) {
      reset();
      invoke_ = other.invoke_;
      destroy_ = other.destroy_;
      move_ = other.move_;
      if (move_) {
        move_(buffer_, other.buffer_);
      }
      other.invoke_ = nullptr;
      other.destroy_ = nullptr;
      other.move_ = nullptr;
    }
    return *this;
  }

  ~small_function() { reset(); }

  void reset()
  {
    if (destroy_) {
      destroy_(buffer_);
      invoke_ = nullptr;
      destroy_ = nullptr;
      move_ = nullptr;
    }
  }

  explicit operator bool() const { return invoke_ != nullptr; }

  Ret operator()(Args... args) const
  {
    return invoke_(buffer_, std::forward<Args>(args)...);
  }
};

/// Entity callback type: void(size_t)
using EntityCallback = small_function<void(size_t), 48>;
/// Waitable callback type: void(size_t, int)
using WaitableCallback = small_function<void(size_t, int), 48>;

}  // namespace rclcpp::executors::cbg::detail

#endif  // RCLCPP__EXECUTORS__CBG__DETAIL__SMALL_FUNCTION_HPP_
