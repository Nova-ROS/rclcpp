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

#ifndef RCLCPP__EXECUTORS__CBG__DETAIL__CBG_HANDLE_POOL_HPP_
#define RCLCPP__EXECUTORS__CBG__DETAIL__CBG_HANDLE_POOL_HPP_

#include <cstddef>
#include <memory>
#include <vector>

#include "rclcpp/executors/cbg/cbg_handle.hpp"
#include "rclcpp/executors/cbg/detail/alloc_vector.hpp"

namespace rclcpp::executors::cbg::detail {

/// Object pool for CBGHandle instances. Uses allocator for construction/destruction.
template<typename AllocatorT = std::allocator<void>>
class CBGHandlePool
{
  using HandleType = CBGHandle<AllocatorT>;
  using HandleAlloc = typename std::allocator_traits<AllocatorT>::template rebind_alloc<HandleType>;

public:
  explicit CBGHandlePool(size_t /*reserve_count*/ = 16, const AllocatorT & alloc = {})
  : alloc_(alloc)
  {}

  /// Allocate and construct a CBGHandle.
  template<typename... Args>
  HandleType * create(Args &&... args)
  {
    HandleType * ptr = std::allocator_traits<HandleAlloc>::allocate(alloc_, 1);
    std::allocator_traits<HandleAlloc>::construct(alloc_, ptr, std::forward<Args>(args)...);
    return ptr;
  }

  /// Destroy and deallocate a CBGHandle.
  void destroy(HandleType * ptr)
  {
    std::allocator_traits<HandleAlloc>::destroy(alloc_, ptr);
    std::allocator_traits<HandleAlloc>::deallocate(alloc_, ptr, 1);
  }

private:
  HandleAlloc alloc_;
};

}  // namespace rclcpp::executors::cbg::detail

#endif  // RCLCPP__EXECUTORS__CBG__DETAIL__CBG_HANDLE_POOL_HPP_
