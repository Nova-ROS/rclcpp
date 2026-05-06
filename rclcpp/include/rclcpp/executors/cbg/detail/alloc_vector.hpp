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

#ifndef RCLCPP__EXECUTORS__CBG__DETAIL__ALLOC_VECTOR_HPP_
#define RCLCPP__EXECUTORS__CBG__DETAIL__ALLOC_VECTOR_HPP_

#include <memory>
#include <vector>

namespace rclcpp::executors::cbg::detail {

/// Allocator-aware vector: rebinds Alloc to element type T.
template<typename T, typename Alloc>
using AllocVector = std::vector<T, typename std::allocator_traits<Alloc>::template rebind_alloc<T>>;

}  // namespace rclcpp::executors::cbg::detail

#endif  // RCLCPP__EXECUTORS__CBG__DETAIL__ALLOC_VECTOR_HPP_
