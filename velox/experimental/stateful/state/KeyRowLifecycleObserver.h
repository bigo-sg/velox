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

namespace facebook::velox::stateful {

/// Reports (key, namespace) entry existence changes to the owner of the
/// key rows. The state tables of the heap backend notify their observer —
/// the backend's key table — so it can count, per key row, how many state
/// entries reference it across all states, and physically reclaim rows
/// that nothing references any more (see KeyTable::advance()). An entry
/// overwritten in place is neither added nor removed: the notifications
/// track existence, not writes. Maps without an observer (the default) do
/// no bookkeeping.
/// @param <K> type of key, a StateKey subclass
template <typename K>
class KeyRowLifecycleObserver {
 public:
  virtual ~KeyRowLifecycleObserver() = default;

  /// A (key, namespace) entry went from absent to present.
  virtual void onEntryAdded(const K& key) = 0;

  /// A (key, namespace) entry that was present was removed.
  virtual void onEntryRemoved(const K& key) = 0;
};

} // namespace facebook::velox::stateful
