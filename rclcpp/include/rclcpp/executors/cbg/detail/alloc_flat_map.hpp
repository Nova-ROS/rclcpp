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

#ifndef RCLCPP__EXECUTORS__CBG__DETAIL__ALLOC_FLAT_MAP_HPP_
#define RCLCPP__EXECUTORS__CBG__DETAIL__ALLOC_FLAT_MAP_HPP_

#include <algorithm>
#include <memory>
#include <utility>
#include <vector>

#include "rclcpp/executors/cbg/detail/alloc_vector.hpp"

namespace rclcpp::executors::cbg::detail {

/// Sorted-vector-based flat map. Contiguous memory, cache-friendly.
/// Uses allocator rebinding for internal storage.
template<typename Key, typename Value, typename Alloc = std::allocator<void>>
class AllocFlatMap
{
  using Entry = std::pair<Key, Value>;
  using Storage = AllocVector<Entry, Alloc>;

  struct KeyComp
  {
    bool operator()(const Entry & a, const Key & k) const {return a.first < k;}
    bool operator()(const Key & k, const Entry & a) const {return k < a.first;}
  };

public:
  explicit AllocFlatMap(const Alloc & alloc = {}) : storage_(alloc) {}

  void reserve(size_t n) { storage_.reserve(n); }

  std::pair<Value *, bool> emplace(Key key, Value value)
  {
    auto it = std::lower_bound(storage_.begin(), storage_.end(), key, KeyComp());
    if (it != storage_.end() && it->first == key) {
      return {&it->second, false};
    }
    it = storage_.emplace(it, std::move(key), std::move(value));
    return {&it->second, true};
  }

  Value * find(const Key & key)
  {
    auto it = std::lower_bound(storage_.begin(), storage_.end(), key, KeyComp());
    if (it != storage_.end() && it->first == key) {
      return &it->second;
    }
    return nullptr;
  }

  const Value * find(const Key & key) const
  {
    auto it = std::lower_bound(storage_.cbegin(), storage_.cend(), key, KeyComp());
    if (it != storage_.cend() && it->first == key) {
      return &it->second;
    }
    return nullptr;
  }

  bool erase(const Key & key)
  {
    auto it = std::lower_bound(storage_.begin(), storage_.end(), key, KeyComp());
    if (it != storage_.end() && it->first == key) {
      storage_.erase(it);
      return true;
    }
    return false;
  }

  size_t size() const { return storage_.size(); }
  bool empty() const { return storage_.empty(); }
  void clear() { storage_.clear(); }

  auto begin() { return storage_.begin(); }
  auto end() { return storage_.end(); }
  auto begin() const { return storage_.begin(); }
  auto end() const { return storage_.end(); }

private:
  Storage storage_;
};

}  // namespace rclcpp::executors::cbg::detail

#endif  // RCLCPP__EXECUTORS__CBG__DETAIL__ALLOC_FLAT_MAP_HPP_
