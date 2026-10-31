/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include "velox/type/Timestamp.h"

namespace facebook::velox::functions::flinksql {

// Per-record wall clock shared by the flinksql temporal functions
// (CURRENT_TIMESTAMP / NOW / LOCALTIMESTAMP / LOCALTIME).
//
// Flink streaming semantics evaluate these functions once per input record:
// every occurrence in the same record observes the same instant, and
// LOCALTIMESTAMP / LOCALTIME derive from it. In the GFV stateful pipeline the
// record-emission boundary is StatefulOperator::pushOutput — the single
// funnel through which a record leaves an operator and the downstream
// evaluation is driven synchronously on the emitting thread — so the clock
// is refreshed there before any downstream temporal evaluation runs.
//
// The state is per-thread (thread_local): a record is emitted and evaluated
// on the same thread, including processing-time timer emissions from the
// scheduler thread. A thread that never crosses an emission boundary (a
// plain velox driver, a direct gtest) reads the unpinned fallback: a fresh
// Timestamp::now() per call, which is the pre-RecordClock behavior.
class RecordClock {
 public:
  // Pins the calling thread's clock to the current instant. Called at every
  // record-emission boundary (StatefulOperator::pushOutput).
  static void refresh() {
    auto& s = state();
    s.instant = Timestamp::now();
    s.pinned = true;
  }

  // Returns the pinned instant when this thread has one, else a fresh
  // wall-clock reading per call (unpinned fallback).
  static Timestamp current() {
    const auto& s = state();
    return s.pinned ? s.instant : Timestamp::now();
  }

  // Drops the pin, restoring the unpinned fallback behavior. Test isolation
  // only: production code never resets.
  static void reset() {
    state().pinned = false;
  }

 private:
  struct State {
    Timestamp instant;
    bool pinned{false};
  };

  static State& state() {
    // Function-local static thread_local: per-thread storage constructed
    // lazily once, ODR-safe (same shape as ProcessingTimeScheduler's
    // generateTimerTaskName generator).
    static thread_local State s;
    return s;
  }
};

} // namespace facebook::velox::functions::flinksql
