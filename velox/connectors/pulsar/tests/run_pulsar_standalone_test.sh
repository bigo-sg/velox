#!/usr/bin/env bash
# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
set -euo pipefail

test_binary="$1"
work_dir="${2:-/tmp/velox-pulsar-standalone-test}"
pulsar_version="${PULSAR_STANDALONE_VERSION:-3.3.0}"
pulsar_url="${PULSAR_STANDALONE_URL:-https://archive.apache.org/dist/pulsar/pulsar-${pulsar_version}/apache-pulsar-${pulsar_version}-bin.tar.gz}"
pulsar_home="${PULSAR_STANDALONE_HOME:-${work_dir}/apache-pulsar-${pulsar_version}}"

mkdir -p "${work_dir}"

if [[ ! -x "${pulsar_home}/bin/pulsar" ]]; then
  archive="${work_dir}/apache-pulsar-${pulsar_version}-bin.tar.gz"
  if [[ ! -f "${archive}" ]] || ! tar -tzf "${archive}" >/dev/null 2>&1; then
    curl -fL "${pulsar_url}" -o "${archive}.tmp"
    mv "${archive}.tmp" "${archive}"
  fi
  tar -xzf "${archive}" -C "${work_dir}"
fi

standalone_log="${work_dir}/pulsar-standalone.log"
"${pulsar_home}/bin/pulsar" standalone >"${standalone_log}" 2>&1 &
standalone_pid=$!

cleanup() {
  kill "${standalone_pid}" >/dev/null 2>&1 || true
  wait "${standalone_pid}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

for _ in $(seq 1 60); do
  if "${pulsar_home}/bin/pulsar-admin" brokers healthcheck >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

"${pulsar_home}/bin/pulsar-admin" brokers healthcheck
PULSAR_SERVICE_URL="${PULSAR_SERVICE_URL:-pulsar://127.0.0.1:6650}" \
  PULSAR_STANDALONE_HOME="${pulsar_home}" \
  "${test_binary}"
