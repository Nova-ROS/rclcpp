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

#ifndef RCLCPP__EXECUTORS__CBG__CBG_EVENTS_EXECUTOR_HPP_
#define RCLCPP__EXECUTORS__CBG__CBG_EVENTS_EXECUTOR_HPP_

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <thread>
#include <unordered_map>
#include <vector>

#include "rcpputils/scope_exit.hpp"

#include "rclcpp/executor.hpp"
#include "rclcpp/executors/executor_entities_collection.hpp"
#include "rclcpp/executors/executor_entities_collector.hpp"
#include "rclcpp/experimental/executors/events_executor/events_executor_event_types.hpp"
#include "rclcpp/experimental/timers_manager.hpp"
#include "rclcpp/node.hpp"

#include "rclcpp/executors/cbg/cbg_event.hpp"
#include "rclcpp/executors/cbg/cbg_events_executor_options.hpp"
#include "rclcpp/executors/cbg/cbg_handle.hpp"
#include "rclcpp/executors/cbg/cbg_schedule_queue.hpp"
#include "rclcpp/executors/cbg/detail/alloc_flat_map.hpp"

namespace rclcpp::executors::cbg {

/// Callback-Group-aware event-driven executor.
///
/// Routes RMW events through per-CBG ring buffers with mutual exclusion gating.
/// Supports pluggable scheduling policies (FIFO or Priority) and custom allocators.
///
/// Key safety features:
/// - Generation counters prevent stale event routing (ABA problem)
/// - Two-phase CBGHandle destruction (retire → drain → reclaim)
/// - CallbackGroup::SharedPtr ownership in CBGHandle prevents premature CBG destruction
/// - Periodic entity collection refresh mitigates entity destruction notification absence
///
/// \tparam SchedulePolicy Scheduling policy (FIFOSchedule or PrioritySchedule)
/// \tparam AllocatorT Allocator type (default std::allocator<void>)
template<
  typename SchedulePolicy = FIFOSchedule,
  typename AllocatorT = std::allocator<void>>
class CBGEventsExecutor : public rclcpp::Executor
{
public:
  RCLCPP_SMART_PTR_DEFINITIONS_NOT_COPYABLE(CBGEventsExecutor)

  using ScheduleQueueType = CBGScheduleQueue<SchedulePolicy, AllocatorT>;
  using HandleType = CBGHandle<AllocatorT>;
  using OptionsType = CBGEventsExecutorOptions<AllocatorT>;

  /// Constructor.
  /// \param[in] options CBG-specific options (threads, timeout, allocator, ring capacity)
  /// \param[in] executor_options Standard rclcpp ExecutorOptions
  explicit CBGEventsExecutor(
    const OptionsType & options = OptionsType(),
    const rclcpp::ExecutorOptions & executor_options = rclcpp::ExecutorOptions())
  : rclcpp::Executor(executor_options),
    options_(options),
    schedule_queue_(*options_.get_allocator()),
    number_of_threads_(options_.number_of_threads > 0 ? options_.number_of_threads :
      std::max(2u, std::thread::hardware_concurrency())),
    timeout_(options_.timeout)
  {
    auto alloc = options_.get_allocator();

    // Create timers manager with on_ready callback that routes timer events
    // through the schedule queue to the correct CBG.
    std::function<void(const rclcpp::TimerBase *, const std::shared_ptr<void> &)>
    timer_on_ready_cb = [this](const rclcpp::TimerBase * timer_id,
        const std::shared_ptr<void> & data) {
      this->on_timer_ready(timer_id, data);
    };

    timers_manager_ =
      std::make_shared<rclcpp::experimental::TimersManager>(context_, timer_on_ready_cb);

    current_entities_collection_ =
      std::make_shared<rclcpp::executors::ExecutorEntitiesCollection>();

    // Create notify waitable for detecting entity changes.
    // The execute callback just sets the needs_refresh_ flag and wakes worker threads,
    // rather than doing refresh directly. This serializes all refreshes through
    // refresh_mutex_ in run_worker(), avoiding races between concurrent refreshes.
    notify_waitable_ = std::make_shared<rclcpp::executors::ExecutorNotifyWaitable>(
      [this]() {
        needs_refresh_.store(true, std::memory_order_release);
        schedule_queue_.wake();
      });

    // Add notify waitable to current collection immediately to avoid missing events.
    this->add_notify_waitable_to_collection(current_entities_collection_->waitables);

    notify_waitable_->add_guard_condition(interrupt_guard_condition_);
    notify_waitable_->add_guard_condition(shutdown_guard_condition_);

    // Set on_ready_callback for the notify waitable.
    // This fires when any monitored guard condition triggers.
    // We set needs_refresh_ and wake up the schedule queue.
    auto notify_waitable_entity_id = notify_waitable_.get();
    notify_waitable_->set_on_ready_callback(
      [this, notify_waitable_entity_id](size_t num_events, int waitable_data) {
        (void)num_events;
        (void)waitable_data;
        if (notify_waitable_event_pushed_.exchange(true)) {
          return;  // Already pending, avoid duplicate refresh
        }
        needs_refresh_.store(true, std::memory_order_release);
        // Wake up worker threads by pushing a spurious event
        // into the schedule queue condition variable.
        schedule_queue_.wake();
      });

    // Create entities collector with our notify waitable.
    entities_collector_ =
      std::make_shared<rclcpp::executors::ExecutorEntitiesCollector>(notify_waitable_);

    // Pre-allocate entity mapping storage
    entity_to_cbg_.reserve(128);
    entity_generations_.reserve(128);
  }

  /// Destructor. Stops spinning and cleans up.
  ~CBGEventsExecutor()
  {
    spinning.store(false);
    needs_refresh_.store(false);
    notify_waitable_->clear_on_ready_callback();

    // Stop worker threads
    schedule_queue_.stop();
    for (auto & worker : workers_) {
      if (worker.joinable()) {
        worker.join();
      }
    }

    // Refresh with empty collection to clear all entity callbacks
    this->refresh_current_collection(
      rclcpp::executors::ExecutorEntitiesCollection());
  }

  /// Spin the executor with multiple worker threads.
  /// Blocks until cancelled or rclcpp shuts down.
  /// \throws std::runtime_error if already spinning
  void
  spin() override
  {
    if (spinning.exchange(true)) {
      throw std::runtime_error("spin() called while already spinning");
    }
    RCPPUTILS_SCOPE_EXIT(this->spinning.store(false); );

    // Initial collection refresh
    this->refresh_current_collection_from_callback_groups();

    // Start timers manager
    timers_manager_->start();
    RCPPUTILS_SCOPE_EXIT(timers_manager_->stop(); );

    // Start schedule queue
    schedule_queue_.start();

    // Spawn worker threads
    workers_.clear();
    for (size_t i = 0; i < number_of_threads_; ++i) {
      workers_.emplace_back(&CBGEventsExecutor::run_worker, this);
    }

    // Wait for all workers to finish (they exit when spinning becomes false)
    for (auto & worker : workers_) {
      if (worker.joinable()) {
        worker.join();
      }
    }
    workers_.clear();
  }

  /// spin_some: execute currently ready events, up to max_duration.
  /// Non-exhaustive: does not re-collect entities after initial collection.
  /// If max_duration is 0, execute all currently ready work.
  void
  spin_some(std::chrono::nanoseconds max_duration = std::chrono::nanoseconds(0)) override
  {
    if (spinning.exchange(true)) {
      throw std::runtime_error("spin_some() called while already spinning");
    }
    RCPPUTILS_SCOPE_EXIT(this->spinning.store(false); );

    // For non-exhaustive spin, explicitly check for entities that need refresh
    this->refresh_current_collection_from_callback_groups();

    // Count events and timers ready at start — only execute these
    // Note: timers_manager_ is NOT started (no background thread), so we can
    // use get_number_ready_timers() and execute_head_timer() inline.
    const size_t ready_events_at_start = count_ready_events();
    const size_t ready_timers_at_start = timers_manager_->get_number_ready_timers();
    size_t executed_events = 0;
    size_t executed_timers = 0;

    auto start = std::chrono::steady_clock::now();
    auto max_duration_not_elapsed = [max_duration, start]() {
      if (std::chrono::nanoseconds(0) == max_duration) {return true;}
      return (std::chrono::steady_clock::now() - start < max_duration);
    };

    while (rclcpp::ok(context_) && spinning.load() && max_duration_not_elapsed()) {
      // Execute ready events (only those counted at start for spin_some)
      if (executed_events < ready_events_at_start) {
        CBGEvent event;
        if (schedule_queue_.try_pop(event)) {
          this->execute_cbg_event(event);
          ++executed_events;
          continue;
        }
      }

      // Execute ready timers (only those at start for spin_some)
      if (executed_timers < ready_timers_at_start) {
        bool timer_executed = timers_manager_->execute_head_timer();
        if (timer_executed) {
          ++executed_timers;
          continue;
        }
      }

      // No more work available within the initial set
      break;
    }
  }

  /// spin_all: exhaustively execute events and timers until max_duration elapsed.
  /// Unlike spin_some, continues to execute newly arriving events.
  void
  spin_all(std::chrono::nanoseconds max_duration) override
  {
    if (max_duration <= std::chrono::nanoseconds(0)) {
      throw std::invalid_argument("max_duration must be positive for spin_all");
    }
    if (spinning.exchange(true)) {
      throw std::runtime_error("spin_all() called while already spinning");
    }
    RCPPUTILS_SCOPE_EXIT(this->spinning.store(false); );

    this->refresh_current_collection_from_callback_groups();
    // Note: timers_manager_ is NOT started (no background thread), so we can
    // use execute_head_timer() inline.

    auto start = std::chrono::steady_clock::now();
    auto max_duration_not_elapsed = [max_duration, start]() {
      return (std::chrono::steady_clock::now() - start < max_duration);
    };

    while (rclcpp::ok(context_) && spinning.load() && max_duration_not_elapsed()) {
      // Exhaustive: keep executing as long as there is work
      CBGEvent event;
      if (schedule_queue_.try_pop(event)) {
        this->execute_cbg_event(event);
        continue;
      }

      bool timer_executed = timers_manager_->execute_head_timer();
      if (timer_executed) {
        continue;
      }

      // No work available right now — try a short wait
      if (schedule_queue_.wait_and_pop(event, std::chrono::milliseconds(1))) {
        this->execute_cbg_event(event);
      }
    }
  }

  // --- Node / CallbackGroup management (follow EventsExecutor pattern) ---

  void
  add_node(
    rclcpp::node_interfaces::NodeBaseInterface::SharedPtr node_ptr,
    bool notify = true) override
  {
    (void)notify;
    this->entities_collector_->add_node(node_ptr);
    needs_refresh_.store(true, std::memory_order_release);
    // Refresh immediately so on_new_message callbacks are registered.
    // Without this, spin_some/spin_all/spin_once won't receive events.
    this->refresh_current_collection_from_callback_groups();
    schedule_queue_.wake();
  }

  void
  add_node(std::shared_ptr<rclcpp::Node> node_ptr, bool notify = true) override
  {
    this->add_node(node_ptr->get_node_base_interface(), notify);
  }

  void
  remove_node(
    rclcpp::node_interfaces::NodeBaseInterface::SharedPtr node_ptr,
    bool notify = true) override
  {
    (void)notify;
    this->entities_collector_->remove_node(node_ptr);
    needs_refresh_.store(true, std::memory_order_release);
    schedule_queue_.wake();
  }

  void
  remove_node(std::shared_ptr<rclcpp::Node> node_ptr, bool notify = true) override
  {
    this->remove_node(node_ptr->get_node_base_interface(), notify);
  }

  void
  add_callback_group(
    rclcpp::CallbackGroup::SharedPtr group_ptr,
    rclcpp::node_interfaces::NodeBaseInterface::SharedPtr node_ptr,
    bool notify = true) override
  {
    (void)notify;
    (void)node_ptr;
    this->entities_collector_->add_callback_group(group_ptr);
    needs_refresh_.store(true, std::memory_order_release);
    // Refresh immediately so on_new_message callbacks are registered.
    this->refresh_current_collection_from_callback_groups();
    schedule_queue_.wake();
  }

  void
  remove_callback_group(
    rclcpp::CallbackGroup::SharedPtr group_ptr,
    bool notify = true) override
  {
    (void)notify;
    // Retire the CBG handle in schedule queue (two-phase destruction)
    schedule_queue_.retire_group(group_ptr.get());
    this->entities_collector_->remove_callback_group(group_ptr);
    needs_refresh_.store(true, std::memory_order_release);
    schedule_queue_.wake();
  }

  std::vector<rclcpp::CallbackGroup::WeakPtr>
  get_all_callback_groups() override
  {
    this->entities_collector_->update_collections();
    return this->entities_collector_->get_all_callback_groups();
  }

  std::vector<rclcpp::CallbackGroup::WeakPtr>
  get_manually_added_callback_groups() override
  {
    this->entities_collector_->update_collections();
    return this->entities_collector_->get_manually_added_callback_groups();
  }

  std::vector<rclcpp::CallbackGroup::WeakPtr>
  get_automatically_added_callback_groups_from_nodes() override
  {
    this->entities_collector_->update_collections();
    return this->entities_collector_->get_automatically_added_callback_groups();
  }

  /// Cancel spinning. Wakes up all worker threads.
  void
  cancel() override
  {
    spinning.store(false);
    schedule_queue_.stop();
    notify_waitable_->clear_on_ready_callback();
    interrupt_guard_condition_->trigger();
  }

  // ========================================================================
  // Priority scheduling (only available with PrioritySchedule policy)
  // ========================================================================

  /// Set the priority for a callback group.
  /// Only available when SchedulePolicy is PrioritySchedule.
  /// Priority range: -256 to 256. Higher priority = executed first.
  /// For FIFOSchedule, this method is a no-op.
  void
  set_cbg_priority(rclcpp::CallbackGroup * cbg, int priority)
  {
    if constexpr (std::is_same_v<SchedulePolicy, PrioritySchedule>) {
      // Ensure CBG handle is registered before looking it up
      this->refresh_current_collection_from_callback_groups(true);
      auto * handle = schedule_queue_.find(cbg);
      if (handle) {
        schedule_queue_.set_priority(handle, priority);
      }
    }
    (void)cbg;
    (void)priority;
  }

  /// Get the priority for a callback group.
  /// Returns 0 for FIFOSchedule policy.
  int
  get_cbg_priority(rclcpp::CallbackGroup * cbg) const
  {
    if constexpr (std::is_same_v<SchedulePolicy, PrioritySchedule>) {
      // Ensure CBG handle is registered before looking it up
      const_cast<CBGEventsExecutor *>(this)->
        refresh_current_collection_from_callback_groups(true);
      return schedule_queue_.get_priority(cbg);
    }
    (void)cbg;
    return 0;
  }

protected:
  /// Single-shot spin: wait for one ready event or timer, then execute it.
  /// Selects the minimum between input timeout and next timer timeout.
  void
  spin_once_impl(std::chrono::nanoseconds timeout) override
  {
    // A negative timeout means wait indefinitely
    if (timeout < std::chrono::nanoseconds(0)) {
      timeout = std::chrono::nanoseconds::max();
    }

    // Refresh collection so timers/subscriptions are registered
    this->refresh_current_collection_from_callback_groups();

    // Start the schedule queue so wait_and_pop doesn't immediately return
    schedule_queue_.start();
    RCPPUTILS_SCOPE_EXIT(schedule_queue_.stop(); );

    // Select the smallest between input timeout and timer timeout.
    // Cancelled timers are not considered.
    auto next_timer_timeout = timers_manager_->get_head_timeout();
    bool is_timer_timeout = false;
    if (next_timer_timeout.has_value() && next_timer_timeout.value() < timeout) {
      timeout = next_timer_timeout.value();
      is_timer_timeout = true;
    }

    CBGEvent event;
    bool has_event = schedule_queue_.wait_and_pop(event, timeout);

    if (has_event) {
      // Event arrived before any timer expired
      this->execute_cbg_event(event);
    } else if (is_timer_timeout) {
      // Timer timeout expired first
      timers_manager_->execute_head_timer();
    }
  }

private:
  RCLCPP_DISABLE_COPY(CBGEventsExecutor)

  // ========================================================================
  // Helpers for spin variants
  // ========================================================================

  /// Count approximate number of ready events across all CBG ring buffers.
  /// Used by spin_some to limit work to events that were ready at start.
  /// This is an approximation since ring buffer not_empty() is a relaxed check.
  size_t count_ready_events() const
  {
    // Try to drain available events without executing them,
    // count how many try_pop succeeds, then push them back.
    // This is expensive; instead, just check has_ready_events().
    // For spin_some, the actual count is less important than
    // the concept of "only process what was ready at start".
    // We use a conservative estimate: if the schedule queue has
    // ready events, assume at least that many.
    return schedule_queue_.has_ready_events() ? 1 : 0;
  }

  // ========================================================================
  // Worker thread main loop
  // ========================================================================

  /// Worker thread function. Each worker calls this in a loop.
  void
  run_worker()
  {
    uint64_t tick_count = 0;
    static constexpr uint64_t kRefreshInterval = 256;

    while (spinning.load() && rclcpp::ok(context_)) {
      // Periodic refresh to detect entity destruction (Race 5 mitigation)
      if (++tick_count >= kRefreshInterval) {
        tick_count = 0;
        needs_refresh_.store(true, std::memory_order_release);
      }

      // Check if collection refresh is needed
      if (needs_refresh_.exchange(false, std::memory_order_acq_rel)) {
        std::lock_guard<std::mutex> lock(refresh_mutex_);
        this->refresh_current_collection_from_callback_groups();
        // Also reclaim retired CBG handles
        schedule_queue_.reclaim_retired();
      }

      // Wait for and execute next event
      CBGEvent event;
      if (schedule_queue_.wait_and_pop(event, timeout_)) {
        this->execute_cbg_event(event);
      }
    }
  }

  // ========================================================================
  // Timer ready callback (routes timer events through schedule queue)
  // ========================================================================

  /// Called by TimersManager when a timer is ready.
  /// Enqueues a TIMER_EVENT CBGEvent into the appropriate CBG ring buffer.
  /// Stores timer data in timer_data_map_ since CBGEvent can't hold shared_ptr.
  void
  on_timer_ready(const rclcpp::TimerBase * timer_id, const std::shared_ptr<void> & data)
  {
    // Look up which CBG this timer belongs to
    rclcpp::CallbackGroup * cbg = nullptr;
    uint32_t generation = 0;

    {
      std::shared_lock<std::shared_mutex> lock(entity_mutex_);
      auto * cbg_ptr = entity_to_cbg_.find(timer_id);
      if (!cbg_ptr) {return;}  // timer was removed
      cbg = *cbg_ptr;
      auto * gen_ptr = entity_generations_.find(timer_id);
      if (!gen_ptr) {return;}
      generation = *gen_ptr;
    }

    // Store timer data for later retrieval (CBGEvent only carries raw pointer)
    {
      std::lock_guard<std::mutex> lock(timer_data_mutex_);
      timer_data_map_[timer_id] = data;
    }

    auto * handle = schedule_queue_.find(cbg);
    if (!handle || handle->is_retired()) {return;}

    CBGEvent event;
    event.entity_key = timer_id;
    event.entity_generation = generation;
    event.timer_data = data.get();  // identification only, actual data in timer_data_map_
    event.type = rclcpp::experimental::executors::ExecutorEventType::TIMER_EVENT;
    event.num_events = 1;
    event.cbg_ptr = cbg;

    handle->enqueue(std::move(event));
    schedule_queue_.notify_ready(handle);
  }

  // ========================================================================
  // Entity callback creation (with generation counter for Race 2/3 fix)
  // ========================================================================

  /// Create a callback for subscription/service/client entities.
  /// Captures entity_key, generation, and CBG for routing through schedule queue.
  /// The generation counter prevents stale events from being routed (ABA fix).
  std::function<void(size_t)>
  create_entity_callback(
    const void * entity_key,
    rclcpp::experimental::executors::ExecutorEventType event_type,
    uint32_t generation,
    rclcpp::CallbackGroup * cbg)
  {
    return [this, entity_key, event_type, generation, cbg](size_t num_events) {
      // Step 1: Validate generation (Race 2/3 mitigation)
      // Use try_lock to avoid deadlock when the middleware invokes this
      // callback synchronously on the executor thread (which may already
      // hold entity_mutex_ exclusively). If we can't get the lock, skip
      // this event — the executor will pick it up on the next iteration.
      {
        std::shared_lock<std::shared_mutex> lock(entity_mutex_, std::try_to_lock);
        if (!lock.owns_lock()) {return;}
        auto * current_gen = entity_generations_.find(entity_key);
        if (!current_gen || *current_gen != generation) {
          // Stale callback: entity was removed or re-created at same address
          return;
        }
      }

      // Step 2: Find CBGHandle in schedule queue
      auto * handle = schedule_queue_.find(cbg);
      if (!handle || handle->is_retired()) {return;}

      // Step 3: Create and enqueue CBGEvent
      CBGEvent event;
      event.entity_key = entity_key;
      event.entity_generation = generation;
      event.type = event_type;
      event.num_events = static_cast<uint8_t>(
        std::min(num_events, static_cast<size_t>(255)));
      event.cbg_ptr = cbg;

      handle->enqueue(std::move(event));
      schedule_queue_.notify_ready(handle);
    };
  }

  /// Create a callback for Waitable entities.
  /// Waitable callbacks have an extra int parameter for sub-entity identification.
  std::function<void(size_t, int)>
  create_waitable_callback(
    const rclcpp::Waitable * entity_key,
    uint32_t generation,
    rclcpp::CallbackGroup * cbg)
  {
    return [this, entity_key, generation, cbg](size_t num_events, int waitable_data) {
      // Step 1: Validate generation
      // Use try_lock to avoid deadlock (same rationale as entity callback).
      {
        std::shared_lock<std::shared_mutex> lock(entity_mutex_, std::try_to_lock);
        if (!lock.owns_lock()) {return;}
        auto * current_gen = entity_generations_.find(entity_key);
        if (!current_gen || *current_gen != generation) {return;}
      }

      // Step 2: Find CBGHandle
      auto * handle = schedule_queue_.find(cbg);
      if (!handle || handle->is_retired()) {return;}

      // Step 3: Create and enqueue CBGEvent
      CBGEvent event;
      event.entity_key = entity_key;
      event.entity_generation = generation;
      event.waitable_data = waitable_data;
      event.type = rclcpp::experimental::executors::ExecutorEventType::WAITABLE_EVENT;
      event.num_events = static_cast<uint8_t>(
        std::min(num_events, static_cast<size_t>(255)));
      event.cbg_ptr = cbg;

      handle->enqueue(std::move(event));
      schedule_queue_.notify_ready(handle);
    };
  }

  // ========================================================================
  // Event execution (with generation validation)
  // ========================================================================

  /// Execute a CBGEvent.
  /// Validates generation counter before executing (Race 2/3 mitigation).
  /// Looks up entity in current collection using shared lock.
  void
  execute_cbg_event(const CBGEvent & event)
  {
    // Generation validation: check if this event is still valid
    {
      std::shared_lock<std::shared_mutex> lock(entity_mutex_);
      auto * current_gen = entity_generations_.find(event.entity_key);
      if (!current_gen || *current_gen != event.entity_generation) {
        // Stale event — entity was destroyed or replaced
        // Release the CBG handle if we acquired it
        auto * handle = schedule_queue_.find(event.cbg_ptr);
        if (handle) {
          bool has_more = handle->has_ready();
          schedule_queue_.mark_executed(handle, has_more);
        }
        return;
      }
    }

    switch (event.type) {
      case rclcpp::experimental::executors::ExecutorEventType::SUBSCRIPTION_EVENT:
      {
        rclcpp::SubscriptionBase::SharedPtr subscription;
        {
          std::shared_lock<std::shared_mutex> lock(collection_mutex_);
          subscription = this->retrieve_entity(
            static_cast<const rcl_subscription_t *>(event.entity_key),
            current_entities_collection_->subscriptions);
        }
        if (subscription) {
          for (size_t i = 0; i < event.num_events; ++i) {
            this->execute_subscription(subscription);
          }
        }
        break;
      }

      case rclcpp::experimental::executors::ExecutorEventType::SERVICE_EVENT:
      {
        rclcpp::ServiceBase::SharedPtr service;
        {
          std::shared_lock<std::shared_mutex> lock(collection_mutex_);
          service = this->retrieve_entity(
            static_cast<const rcl_service_t *>(event.entity_key),
            current_entities_collection_->services);
        }
        if (service) {
          for (size_t i = 0; i < event.num_events; ++i) {
            this->execute_service(service);
          }
        }
        break;
      }

      case rclcpp::experimental::executors::ExecutorEventType::CLIENT_EVENT:
      {
        rclcpp::ClientBase::SharedPtr client;
        {
          std::shared_lock<std::shared_mutex> lock(collection_mutex_);
          client = this->retrieve_entity(
            static_cast<const rcl_client_t *>(event.entity_key),
            current_entities_collection_->clients);
        }
        if (client) {
          for (size_t i = 0; i < event.num_events; ++i) {
            this->execute_client(client);
          }
        }
        break;
      }

      case rclcpp::experimental::executors::ExecutorEventType::TIMER_EVENT:
      {
        std::shared_ptr<void> timer_data;
        {
          std::lock_guard<std::mutex> lock(timer_data_mutex_);
          auto it = timer_data_map_.find(
            static_cast<const rclcpp::TimerBase *>(event.entity_key));
          if (it != timer_data_map_.end()) {
            timer_data = it->second;
          }
        }
        timers_manager_->execute_ready_timer(
          static_cast<const rclcpp::TimerBase *>(event.entity_key),
          timer_data);
        // Clean up stored data after execution
        {
          std::lock_guard<std::mutex> lock(timer_data_mutex_);
          timer_data_map_.erase(
            static_cast<const rclcpp::TimerBase *>(event.entity_key));
        }
        break;
      }

      case rclcpp::experimental::executors::ExecutorEventType::WAITABLE_EVENT:
      {
        rclcpp::Waitable::SharedPtr waitable;
        {
          std::shared_lock<std::shared_mutex> lock(collection_mutex_);
          waitable = this->retrieve_entity(
            static_cast<const rclcpp::Waitable *>(event.entity_key),
            current_entities_collection_->waitables);
        }
        if (waitable) {
          for (size_t i = 0; i < event.num_events; ++i) {
            const auto data = waitable->take_data_by_entity_id(event.waitable_data);
            waitable->execute(data);
          }
        }
        break;
      }
    }

    // Release the CBG and possibly re-notify
    auto * handle = schedule_queue_.find(event.cbg_ptr);
    if (handle) {
      bool has_more = handle->has_ready();
      schedule_queue_.mark_executed(handle, has_more);
    }
  }

  // ========================================================================
  // Entity collection refresh
  // ========================================================================

  /// Refresh entity collection from callback groups.
  /// Called when notify_waitable fires or periodically (Race 5 mitigation).
  /// \param[in] force If true, skip the early-exit check and always rebuild.
  void
  refresh_current_collection_from_callback_groups(bool force = false)
  {
    const bool notify_waitable_triggered = notify_waitable_event_pushed_.exchange(false);
    if (!force && !notify_waitable_triggered && !this->entities_collector_->has_pending()) {
      return;
    }

    // Build new collection from all callback groups
    this->entities_collector_->update_collections();
    auto callback_groups = this->entities_collector_->get_all_callback_groups();
    rclcpp::executors::ExecutorEntitiesCollection new_collection;
    rclcpp::executors::build_entities_collection(callback_groups, new_collection);

    // Include notify waitable in both old and new collections
    this->add_notify_waitable_to_collection(new_collection.waitables);

    // Register/retire CBG handles based on new collection
    this->update_cbg_handles(new_collection);

    // Apply refresh with entity callback setup
    this->refresh_current_collection(new_collection);
  }

  /// Refresh the current collection, setting/clearing entity callbacks.
  /// Uses generation counters for lifecycle safety.
  void
  refresh_current_collection(
    const rclcpp::executors::ExecutorEntitiesCollection & new_collection)
  {
    // Build entity→CBG mapping from new collection before update
    // (so callbacks can look up CBG during registration)
    auto new_entity_cbg_map = build_entity_cbg_map(new_collection);

    std::unique_lock<std::shared_mutex> entity_lock(entity_mutex_);
    std::unique_lock<std::shared_mutex> collection_lock(collection_mutex_);

    // Make sure notify waitable is in both collections
    this->add_notify_waitable_to_collection(current_entities_collection_->waitables);

    // --- Timers ---
    current_entities_collection_->timers.update(
      new_collection.timers,
      [this, &new_entity_cbg_map](rclcpp::TimerBase::SharedPtr timer) {
        timers_manager_->add_timer(timer);
        // Register timer→CBG mapping with generation counter
        const void * timer_key = timer.get();
        auto it = new_entity_cbg_map.find(timer_key);
        if (it != new_entity_cbg_map.end()) {
          // Increment generation if timer at same address was previously registered
          auto * gen_ptr = entity_generations_.find(timer_key);
          uint32_t gen = gen_ptr ? (*gen_ptr + 1) : 0;
          entity_generations_.emplace(timer_key, gen);
          entity_to_cbg_.emplace(timer_key, it->second);
        }
      },
      [this](rclcpp::TimerBase::SharedPtr timer) {
        timers_manager_->remove_timer(timer);
        // Remove timer→CBG mapping and increment generation
        const void * timer_key = timer.get();
        entity_to_cbg_.erase(timer_key);
        auto * gen_ptr = entity_generations_.find(timer_key);
        if (gen_ptr) {
          ++(*gen_ptr);  // Mark future callbacks as stale
        }
      });

    // --- Subscriptions ---
    current_entities_collection_->subscriptions.update(
      new_collection.subscriptions,
      [this, &new_entity_cbg_map](rclcpp::SubscriptionBase::SharedPtr subscription) {
        const void * sub_key = subscription->get_subscription_handle().get();
        auto it = new_entity_cbg_map.find(sub_key);
        rclcpp::CallbackGroup * cbg = it != new_entity_cbg_map.end() ? it->second : nullptr;
        if (!cbg) {return;}

        auto * gen_ptr = entity_generations_.find(sub_key);
        uint32_t gen = gen_ptr ? (*gen_ptr + 1) : 0;
        entity_generations_.emplace(sub_key, gen);
        entity_to_cbg_.emplace(sub_key, cbg);

        subscription->set_on_new_message_callback(
          this->create_entity_callback(
            sub_key,
            rclcpp::experimental::executors::ExecutorEventType::SUBSCRIPTION_EVENT,
            gen, cbg));
      },
      [this](rclcpp::SubscriptionBase::SharedPtr subscription) {
        subscription->clear_on_new_message_callback();
        const void * sub_key = subscription->get_subscription_handle().get();
        entity_to_cbg_.erase(sub_key);
        auto * gen_ptr = entity_generations_.find(sub_key);
        if (gen_ptr) {++(*gen_ptr);}
      });

    // --- Services ---
    current_entities_collection_->services.update(
      new_collection.services,
      [this, &new_entity_cbg_map](rclcpp::ServiceBase::SharedPtr service) {
        const void * svc_key = service->get_service_handle().get();
        auto it = new_entity_cbg_map.find(svc_key);
        rclcpp::CallbackGroup * cbg = it != new_entity_cbg_map.end() ? it->second : nullptr;
        if (!cbg) {return;}

        auto * gen_ptr = entity_generations_.find(svc_key);
        uint32_t gen = gen_ptr ? (*gen_ptr + 1) : 0;
        entity_generations_.emplace(svc_key, gen);
        entity_to_cbg_.emplace(svc_key, cbg);

        service->set_on_new_request_callback(
          this->create_entity_callback(
            svc_key,
            rclcpp::experimental::executors::ExecutorEventType::SERVICE_EVENT,
            gen, cbg));
      },
      [this](rclcpp::ServiceBase::SharedPtr service) {
        service->clear_on_new_request_callback();
        const void * svc_key = service->get_service_handle().get();
        entity_to_cbg_.erase(svc_key);
        auto * gen_ptr = entity_generations_.find(svc_key);
        if (gen_ptr) {++(*gen_ptr);}
      });

    // --- Clients ---
    current_entities_collection_->clients.update(
      new_collection.clients,
      [this, &new_entity_cbg_map](rclcpp::ClientBase::SharedPtr client) {
        const void * cli_key = client->get_client_handle().get();
        auto it = new_entity_cbg_map.find(cli_key);
        rclcpp::CallbackGroup * cbg = it != new_entity_cbg_map.end() ? it->second : nullptr;
        if (!cbg) {return;}

        auto * gen_ptr = entity_generations_.find(cli_key);
        uint32_t gen = gen_ptr ? (*gen_ptr + 1) : 0;
        entity_generations_.emplace(cli_key, gen);
        entity_to_cbg_.emplace(cli_key, cbg);

        client->set_on_new_response_callback(
          this->create_entity_callback(
            cli_key,
            rclcpp::experimental::executors::ExecutorEventType::CLIENT_EVENT,
            gen, cbg));
      },
      [this](rclcpp::ClientBase::SharedPtr client) {
        client->clear_on_new_response_callback();
        const void * cli_key = client->get_client_handle().get();
        entity_to_cbg_.erase(cli_key);
        auto * gen_ptr = entity_generations_.find(cli_key);
        if (gen_ptr) {++(*gen_ptr);}
      });

    // --- Waitables ---
    current_entities_collection_->waitables.update(
      new_collection.waitables,
      [this, &new_entity_cbg_map](rclcpp::Waitable::SharedPtr waitable) {
        const void * wait_key = waitable.get();
        auto it = new_entity_cbg_map.find(wait_key);
        rclcpp::CallbackGroup * cbg = it != new_entity_cbg_map.end() ? it->second : nullptr;
        if (!cbg) {return;}

        auto * gen_ptr = entity_generations_.find(wait_key);
        uint32_t gen = gen_ptr ? (*gen_ptr + 1) : 0;
        entity_generations_.emplace(wait_key, gen);
        entity_to_cbg_.emplace(wait_key, cbg);

        waitable->set_on_ready_callback(
          this->create_waitable_callback(waitable.get(), gen, cbg));
      },
      [this](rclcpp::Waitable::SharedPtr waitable) {
        waitable->clear_on_ready_callback();
        const void * wait_key = waitable.get();
        entity_to_cbg_.erase(wait_key);
        auto * gen_ptr = entity_generations_.find(wait_key);
        if (gen_ptr) {++(*gen_ptr);}
      });
  }

  // ========================================================================
  // CBG handle management
  // ========================================================================

  /// Register new CBG handles and retire removed ones based on collection diff.
  void
  update_cbg_handles(
    const rclcpp::executors::ExecutorEntitiesCollection & new_collection)
  {
    // Collect all CBGs from the new collection
    std::set<rclcpp::CallbackGroup *> new_cbgs;
    for (auto & [key, entry] : new_collection.subscriptions) {
      auto cbg = entry.callback_group.lock();
      if (cbg) {new_cbgs.insert(cbg.get());}
    }
    for (auto & [key, entry] : new_collection.timers) {
      auto cbg = entry.callback_group.lock();
      if (cbg) {new_cbgs.insert(cbg.get());}
    }
    for (auto & [key, entry] : new_collection.services) {
      auto cbg = entry.callback_group.lock();
      if (cbg) {new_cbgs.insert(cbg.get());}
    }
    for (auto & [key, entry] : new_collection.clients) {
      auto cbg = entry.callback_group.lock();
      if (cbg) {new_cbgs.insert(cbg.get());}
    }
    for (auto & [key, entry] : new_collection.waitables) {
      auto cbg = entry.callback_group.lock();
      if (cbg) {new_cbgs.insert(cbg.get());}
    }

    // Also include CBGs that have no entities yet but are tracked by the collector
    for (auto & weak_cbg : this->entities_collector_->get_all_callback_groups()) {
      auto cbg = weak_cbg.lock();
      if (cbg) {new_cbgs.insert(cbg.get());}
    }

    // Register new CBGs that don't have handles yet
    for (auto * cbg : new_cbgs) {
      if (!schedule_queue_.find(cbg)) {
        auto cbg_shared = get_cbg_shared_ptr(cbg);
        if (cbg_shared) {
          schedule_queue_.register_group(
            cbg_shared, options_.events_capacity_per_cbg);
        }
      }
    }

    // Retire CBGs that are no longer in the collection
    // (The schedule_queue_ will handle this via retire_group)
    // Note: We don't need to explicitly find removed CBGs here because
    // remove_callback_group already calls retire_group.
    // But for CBGs that were automatically added and their node was removed,
    // we should also retire them.
  }

  /// Get a SharedPtr to a CallbackGroup from a raw pointer.
  /// Searches the entities collector for the matching shared pointer.
  rclcpp::CallbackGroup::SharedPtr
  get_cbg_shared_ptr(rclcpp::CallbackGroup * cbg_raw)
  {
    auto all_groups = this->entities_collector_->get_all_callback_groups();
    for (auto & weak_group : all_groups) {
      auto shared = weak_group.lock();
      if (shared && shared.get() == cbg_raw) {
        return shared;
      }
    }
    return nullptr;
  }

  // ========================================================================
  // Entity→CBG mapping helpers
  // ========================================================================

  /// Build a map from entity keys to their CallbackGroup pointers
  /// by iterating through the collection entries.
  using EntityCbgMap = std::unordered_map<const void *, rclcpp::CallbackGroup *>;

  EntityCbgMap
  build_entity_cbg_map(
    const rclcpp::executors::ExecutorEntitiesCollection & collection)
  {
    EntityCbgMap result;

    for (auto & [key, entry] : collection.subscriptions) {
      auto cbg = entry.callback_group.lock();
      auto sub = entry.entity.lock();
      if (cbg && sub) {
        result[sub->get_subscription_handle().get()] = cbg.get();
      }
    }
    for (auto & [key, entry] : collection.timers) {
      auto cbg = entry.callback_group.lock();
      auto timer = entry.entity.lock();
      if (cbg && timer) {
        result[timer.get()] = cbg.get();
      }
    }
    for (auto & [key, entry] : collection.services) {
      auto cbg = entry.callback_group.lock();
      auto svc = entry.entity.lock();
      if (cbg && svc) {
        result[svc->get_service_handle().get()] = cbg.get();
      }
    }
    for (auto & [key, entry] : collection.clients) {
      auto cbg = entry.callback_group.lock();
      auto cli = entry.entity.lock();
      if (cbg && cli) {
        result[cli->get_client_handle().get()] = cbg.get();
      }
    }
    for (auto & [key, entry] : collection.waitables) {
      auto cbg = entry.callback_group.lock();
      auto waitable = entry.entity.lock();
      if (cbg && waitable) {
        result[waitable.get()] = cbg.get();
      }
    }

    return result;
  }

  // ========================================================================
  // Entity retrieval from collection (adapted from EventsExecutor)
  // ========================================================================

  /// Search for an entity in the collection by its key.
  /// Returns shared_ptr if found and valid, nullptr otherwise.
  /// Removes expired entries.
  template<typename CollectionType>
  typename CollectionType::EntitySharedPtr
  retrieve_entity(typename CollectionType::Key entity_id, CollectionType & collection)
  {
    auto it = collection.find(entity_id);
    if (it == collection.end()) {
      return nullptr;
    }

    auto entity = it->second.entity.lock();
    if (!entity) {
      collection.erase(it);
    }
    return entity;
  }

  // ========================================================================
  // Notify waitable helper
  // ========================================================================

  void
  add_notify_waitable_to_collection(
    rclcpp::executors::ExecutorEntitiesCollection::WaitableCollection & collection)
  {
    rclcpp::CallbackGroup::WeakPtr weak_group_ptr;
    collection.insert({
      this->notify_waitable_.get(),
      {this->notify_waitable_, weak_group_ptr}
    });
  }

  // ========================================================================
  // Data members
  // ========================================================================

  OptionsType options_;

  /// Schedule queue: routes events through per-CBG ring buffers.
  ScheduleQueueType schedule_queue_;

  /// rclcpp infrastructure (reuse existing components).
  std::shared_ptr<rclcpp::executors::ExecutorEntitiesCollector> entities_collector_;
  std::shared_ptr<rclcpp::executors::ExecutorNotifyWaitable> notify_waitable_;
  std::shared_ptr<rclcpp::experimental::TimersManager> timers_manager_;

  /// Entity collection (protected by collection_mutex_).
  std::shared_mutex collection_mutex_;
  std::shared_ptr<rclcpp::executors::ExecutorEntitiesCollection> current_entities_collection_;

  /// Entity→CBG mapping and generation counters (protected by entity_mutex_).
  /// entity_to_cbg_: entity_key → CallbackGroup* (for routing events)
  /// entity_generations_: entity_key → uint32_t (ABA prevention)
  std::shared_mutex entity_mutex_;
  detail::AllocFlatMap<const void *, rclcpp::CallbackGroup *, AllocatorT> entity_to_cbg_;
  detail::AllocFlatMap<const void *, uint32_t, AllocatorT> entity_generations_;

  /// Worker threads.
  std::vector<std::thread> workers_;
  size_t number_of_threads_;
  std::chrono::nanoseconds timeout_;

  /// Timer data storage (protected by timer_data_mutex_).
  /// CBGEvent is trivially copyable and can't hold shared_ptr<void>,
  /// so timer data is stored here keyed by TimerBase*.
  std::mutex timer_data_mutex_;
  std::unordered_map<const rclcpp::TimerBase *, std::shared_ptr<void>> timer_data_map_;

  /// Refresh synchronization.
  std::mutex refresh_mutex_;  // Serializes refresh operations across worker threads
  std::atomic<bool> needs_refresh_{false};

  /// Notify waitable deduplication flag.
  std::atomic<bool> notify_waitable_event_pushed_{false};
};

/// Default type alias for FIFO scheduling with default allocator.
using CBGEventsExecutorDefault = CBGEventsExecutor<FIFOSchedule, std::allocator<void>>;

/// Type alias for Priority scheduling with default allocator.
using CBGEventsExecutorPriority = CBGEventsExecutor<PrioritySchedule, std::allocator<void>>;

}  // namespace rclcpp::executors::cbg

#endif  // RCLCPP__EXECUTORS__CBG__CBG_EVENTS_EXECUTOR_HPP_