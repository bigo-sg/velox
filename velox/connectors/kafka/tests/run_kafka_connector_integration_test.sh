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

usage() {
  cat <<'EOF'
Usage:
  run_kafka_connector_integration_test.sh <velox-build-dir>

Environment:
  KAFKA_HOME          Kafka installation directory. If unset, Kafka commands are
                      downloaded from Apache archive into the build directory.
  KAFKA_VERSION       Apache Kafka version to download. Default: 3.9.0
  KAFKA_SCALA_VERSION Scala binary version for Kafka. Default: 2.13
  KAFKA_DOWNLOAD_BASE_URL
                      Apache Kafka download base URL.
                      Default: https://archive.apache.org/dist/kafka
  KAFKA_DOWNLOAD_CACHE
                      Download/extract cache directory. Default:
                      <velox-build-dir>/kafka-download-cache
  KAFKA_TEST_HOST    Kafka host. Default: 127.0.0.1
  KAFKA_TEST_PORT    Kafka broker port. Default: 19092
  KAFKA_TEST_CONTROLLER_PORT
                      Kafka KRaft controller port. Default: 19093
  KAFKA_TEST_TOPIC   Kafka topic used by the connector test. Default: test_kafka
  KAFKA_TEST_FILTER  Optional gtest filter.

Example:
  velox/connectors/kafka/tests/run_kafka_connector_integration_test.sh \
    /tmp/bigo-velox-kafka-ckpt-build8
EOF
}

if [[ $# -ne 1 ]]; then
  usage >&2
  exit 2
fi

BUILD_DIR="$1"
TEST_BINARY="${BUILD_DIR}/velox/connectors/kafka/tests/velox_kafka_connector_test"

if [[ ! -x "${TEST_BINARY}" ]]; then
  echo "Kafka connector test binary not found or not executable: ${TEST_BINARY}" >&2
  echo "Build it first: ninja -C ${BUILD_DIR} velox_kafka_connector_test" >&2
  exit 2
fi

find_kafka_command() {
  local command_name="$1"
  if [[ -n "${KAFKA_HOME:-}" ]]; then
    local command_path="${KAFKA_HOME}/bin/${command_name}"
    if [[ -x "${command_path}" ]]; then
      echo "${command_path}"
      return
    fi
  fi

  if command -v "${command_name}" >/dev/null 2>&1; then
    command -v "${command_name}"
    return
  fi

  echo "Unable to find ${command_name}. Set KAFKA_HOME to a Kafka installation directory." >&2
  exit 2
}

download_file() {
  local url="$1"
  local output="$2"
  if command -v curl >/dev/null 2>&1; then
    curl --fail --location --retry 3 --output "${output}" "${url}"
  elif command -v wget >/dev/null 2>&1; then
    wget --tries=3 --output-document="${output}" "${url}"
  else
    echo "Unable to find curl or wget to download Kafka." >&2
    exit 2
  fi
}

verify_sha512() {
  local archive="$1"
  local checksum_file="$2"
  local checksum_line
  local expected
  local actual
  checksum_line="$(tr -d '\r' <"${checksum_file}" | head -n 1)"
  if [[ "${checksum_line}" == *"="* ]]; then
    expected="${checksum_line##*=}"
  elif [[ "${checksum_line}" == *":"* ]]; then
    expected="$(sed 's/.*: //' "${checksum_file}")"
  else
    expected="$(awk '{print $1}' "${checksum_file}")"
  fi
  expected="$(echo "${expected}" | tr -d '[:space:]' | tr '[:upper:]' '[:lower:]')"
  actual="$(sha512sum "${archive}" | awk '{print $1}' | tr '[:upper:]' '[:lower:]')"
  if [[ "${actual}" != "${expected}" ]]; then
    echo "Kafka archive checksum mismatch: ${archive}" >&2
    exit 1
  fi
}

ensure_kafka_home() {
  if [[ -n "${KAFKA_HOME:-}" ]]; then
    return
  fi

  KAFKA_VERSION="${KAFKA_VERSION:-3.9.0}"
  KAFKA_SCALA_VERSION="${KAFKA_SCALA_VERSION:-2.13}"
  KAFKA_DOWNLOAD_BASE_URL="${KAFKA_DOWNLOAD_BASE_URL:-https://archive.apache.org/dist/kafka}"
  KAFKA_DOWNLOAD_CACHE="${KAFKA_DOWNLOAD_CACHE:-${BUILD_DIR}/kafka-download-cache}"

  local kafka_dir="kafka_${KAFKA_SCALA_VERSION}-${KAFKA_VERSION}"
  local archive="${kafka_dir}.tgz"
  local archive_path="${KAFKA_DOWNLOAD_CACHE}/${archive}"
  local checksum_path="${archive_path}.sha512"
  local archive_url="${KAFKA_DOWNLOAD_BASE_URL}/${KAFKA_VERSION}/${archive}"
  local checksum_url="${archive_url}.sha512"

  mkdir -p "${KAFKA_DOWNLOAD_CACHE}"
  if [[ ! -f "${archive_path}" ]]; then
    download_file "${archive_url}" "${archive_path}"
  fi
  if [[ ! -f "${checksum_path}" ]]; then
    download_file "${checksum_url}" "${checksum_path}"
  fi
  verify_sha512 "${archive_path}" "${checksum_path}"

  if [[ ! -x "${KAFKA_DOWNLOAD_CACHE}/${kafka_dir}/bin/kafka-storage.sh" ]]; then
    tar -xzf "${archive_path}" -C "${KAFKA_DOWNLOAD_CACHE}"
  fi
  KAFKA_HOME="${KAFKA_DOWNLOAD_CACHE}/${kafka_dir}"
}

ensure_kafka_home

KAFKA_STORAGE="$(find_kafka_command kafka-storage.sh)"
KAFKA_SERVER_START="$(find_kafka_command kafka-server-start.sh)"
KAFKA_SERVER_STOP="$(find_kafka_command kafka-server-stop.sh)"
KAFKA_TOPICS="$(find_kafka_command kafka-topics.sh)"

KAFKA_TEST_HOST="${KAFKA_TEST_HOST:-127.0.0.1}"
KAFKA_TEST_PORT="${KAFKA_TEST_PORT:-19092}"
KAFKA_TEST_CONTROLLER_PORT="${KAFKA_TEST_CONTROLLER_PORT:-19093}"
KAFKA_TEST_TOPIC="${KAFKA_TEST_TOPIC:-test_kafka}"
KAFKA_TEST_INSTANCE="${KAFKA_TEST_HOST}:${KAFKA_TEST_PORT}"
KAFKA_JMX_OPTS="${KAFKA_JMX_OPTS:- }"
export KAFKA_JMX_OPTS

TEST_ROOT="$(mktemp -d /tmp/velox-kafka-connector-test.XXXXXX)"
SERVER_PROPERTIES="${TEST_ROOT}/server.properties"
KAFKA_LOG="${TEST_ROOT}/kafka.log"
KAFKA_PID=""

cleanup() {
  local exit_code=$?
  if [[ -n "${KAFKA_PID}" ]] && kill -0 "${KAFKA_PID}" >/dev/null 2>&1; then
    LOG_DIR="${TEST_ROOT}/script-logs" "${KAFKA_SERVER_STOP}" >/dev/null 2>&1 || true
    for _ in {1..30}; do
      if ! kill -0 "${KAFKA_PID}" >/dev/null 2>&1; then
        break
      fi
      sleep 1
    done
    if kill -0 "${KAFKA_PID}" >/dev/null 2>&1; then
      kill "${KAFKA_PID}" >/dev/null 2>&1 || true
    fi
  fi
  rm -rf "${TEST_ROOT}"
  exit "${exit_code}"
}
trap cleanup EXIT INT TERM

mkdir -p "${TEST_ROOT}/script-logs"

cat > "${SERVER_PROPERTIES}" <<EOF
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@${KAFKA_TEST_HOST}:${KAFKA_TEST_CONTROLLER_PORT}
listeners=PLAINTEXT://${KAFKA_TEST_HOST}:${KAFKA_TEST_PORT},CONTROLLER://${KAFKA_TEST_HOST}:${KAFKA_TEST_CONTROLLER_PORT}
advertised.listeners=PLAINTEXT://${KAFKA_TEST_HOST}:${KAFKA_TEST_PORT}
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
inter.broker.listener.name=PLAINTEXT
log.dirs=${TEST_ROOT}/logs
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
auto.create.topics.enable=true
num.partitions=1
EOF

CLUSTER_ID="$("${KAFKA_STORAGE}" random-uuid)"
"${KAFKA_STORAGE}" format -t "${CLUSTER_ID}" -c "${SERVER_PROPERTIES}" >/dev/null

LOG_DIR="${TEST_ROOT}/script-logs" \
  "${KAFKA_SERVER_START}" "${SERVER_PROPERTIES}" >"${KAFKA_LOG}" 2>&1 &
KAFKA_PID="$!"

for _ in {1..60}; do
  if "${KAFKA_TOPICS}" --bootstrap-server "${KAFKA_TEST_INSTANCE}" --list >/dev/null 2>&1; then
    break
  fi
  if ! kill -0 "${KAFKA_PID}" >/dev/null 2>&1; then
    echo "Kafka exited before it became ready. Log:" >&2
    cat "${KAFKA_LOG}" >&2
    exit 1
  fi
  sleep 1
done

if ! "${KAFKA_TOPICS}" \
  --bootstrap-server "${KAFKA_TEST_INSTANCE}" \
  --create \
  --if-not-exists \
  --topic "${KAFKA_TEST_TOPIC}" \
  --partitions 1 \
  --replication-factor 1 >/dev/null 2>&1; then
  "${KAFKA_TOPICS}" \
    --bootstrap-server "${KAFKA_TEST_INSTANCE}" \
    --create \
    --topic "${KAFKA_TEST_TOPIC}" \
    --partitions 1 \
    --replication-factor 1 >/dev/null
fi

cat > "${BUILD_DIR}/kafka.conf" <<EOF
kafka.test.instance=${KAFKA_TEST_INSTANCE}
kafka.test.topic=${KAFKA_TEST_TOPIC}
EOF

if [[ -n "${KAFKA_TEST_FILTER:-}" ]]; then
  (cd "${BUILD_DIR}" && "${TEST_BINARY}" --gtest_filter="${KAFKA_TEST_FILTER}")
else
  (cd "${BUILD_DIR}" && "${TEST_BINARY}")
fi
