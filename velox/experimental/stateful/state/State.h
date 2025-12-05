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

#include <list>
#include <memory>
#include <map>

namespace facebook::velox::stateful {

// This class is relevent to flink org.apache.flink.api.common.State.
class State {
 public:
  static const int VOID_NAMESPACE = 0;
  virtual void clear() = 0;
};

using StatePtr = std::shared_ptr<State>;

// This class is relevent to flink org.apache.flink.api.common.MapState.
template <typename K, typename N, typename UK, typename UV>
class MapState : public State {
 public:
  virtual UV get(K key, N ns, UK userKey) = 0;

  virtual void put(K key, N ns, UK userKey, UV value) = 0;

  virtual std::map<UK, UV> entries(K key, N ns) = 0;

  virtual void remove(K key, N ns, UK userKey) = 0;
};

// This class is relevent to flink org.apache.flink.api.common.ListState.
template <typename K, typename N, typename S>
class ListState : public State {
 public:
  virtual std::list<S>& get(K key, N ns) = 0;

  virtual void add(K key, N ns, S value) = 0;

  virtual void remove(K key, N ns) = 0;
//  virtual void addAll(std::list<T>& values) = 0;

//  virtual void update(std::list<T>& values) = 0;
};

// This class is relevent to flink org.apache.flink.api.common.ListState.
template <typename K, typename N, typename V>
class ValueState : public State {
 public:
  virtual V value(K key, N ns) = 0;

  virtual void update(K key, N ns, V value) = 0;

  virtual void remove(K key, N ns) = 0;
};

} // namespace facebook::velox::stateful
