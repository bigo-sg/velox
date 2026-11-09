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
#include "velox/functions/flinksql/tests/FlinkFunctionBaseTest.h"

#include "velox/common/base/tests/GTestUtils.h"

namespace facebook::velox::functions::flinksql::test {

namespace {
const std::string kFull{"yyyy-MM-dd HH:mm:ss"};
const std::string kIso{"yyyy-MM-dd"};
} // namespace

// Expected values are pinned from Flink's actual behavior where the two
// engines agree, and from this engine's semantics where they differ
// (user-approved TODO-only deviations).
class ToTimestampTest : public FlinkFunctionBaseTest {
 protected:
  std::optional<Timestamp> toTimestampOnce(
      const std::optional<std::string>& s) {
    return evaluateOnce<Timestamp>("to_timestamp(c0)", s);
  }

  // Non-constant format: the per-row build path.
  std::optional<Timestamp> toTimestampOnce(
      const std::optional<std::string>& s,
      const std::optional<std::string>& f) {
    return evaluateOnce<Timestamp>("to_timestamp(c0, c1)", s, f);
  }

  // Constant format: the compile-once path.
  std::optional<Timestamp> toTimestampConstFormatOnce(
      const std::optional<std::string>& s,
      const std::optional<std::string>& f) {
    auto formatFlat = makeNullableFlatVector<std::string>({f});
    auto data = makeRowVector(std::vector<VectorPtr>{
        makeNullableFlatVector<std::string>({s}),
        BaseVector::wrapInConstant(1, 0, formatFlat)});
    return evaluateOnce<Timestamp>("to_timestamp(c0, c1)", data);
  }
};

// A. One-arg basics: the card's 22-row example table plus supplements;
// expected values are Flink-probed (SMART clamping, 24:00 roll, defaults).
TEST_F(ToTimestampTest, OneArgBasics) {
  EXPECT_EQ(
      Timestamp(1789120800, 0),
      toTimestampOnce(std::string("2026-09-11 10:00:00")));
  EXPECT_EQ(
      Timestamp(1788239167, 0), toTimestampOnce(std::string("2026-9-1 5:6:7")));
  EXPECT_EQ(
      Timestamp(1789084800, 0), toTimestampOnce(std::string("2026-09-11")));
  EXPECT_EQ(
      Timestamp(1789120800, 123000000),
      toTimestampOnce(std::string("2026-09-11 10:00:00.123456789")));
  EXPECT_EQ(
      Timestamp(1789120800, 0),
      toTimestampOnce(std::string("2026-09-11 10:00:00.")));
  EXPECT_EQ(
      Timestamp(1789171200, 0),
      toTimestampOnce(std::string("2026-09-11 24:00:00")));
  EXPECT_EQ(
      Timestamp(1772272800, 0),
      toTimestampOnce(std::string("2026-02-30 10:00:00")));
  EXPECT_EQ(
      Timestamp(1709200800, 0),
      toTimestampOnce(std::string("2024-02-30 10:00:00")));
  EXPECT_EQ(
      Timestamp(1777543200, 0),
      toTimestampOnce(std::string("2026-04-31 10:00:00")));
  EXPECT_EQ(Timestamp(1788220800, 0), toTimestampOnce(std::string("2026-09-")));
  EXPECT_EQ(
      Timestamp(253402300800, 0),
      toTimestampOnce(std::string("+10000-01-01 00:00:00")));
  EXPECT_EQ(
      Timestamp(1789120800, 999000000),
      toTimestampOnce(std::string("2026-09-11 10:00:00.999999999")));
  EXPECT_EQ(
      Timestamp(1789120800, 500000000),
      toTimestampOnce(std::string("2026-09-11 10:00:00.5")));
  EXPECT_EQ(
      Timestamp(1789120800, 50000000),
      toTimestampOnce(std::string("2026-09-11 10:00:00.05")));
  EXPECT_EQ(
      Timestamp(1798761599, 999000000),
      toTimestampOnce(std::string("2026-12-31 23:59:59.999")));
  EXPECT_EQ(
      Timestamp(-61861276800, 0),
      toTimestampOnce(std::string("0009-09-11 00:00:00")));
  EXPECT_EQ(
      Timestamp(1789120800, 0),
      toTimestampOnce(std::string("2026-09-11 10:00:0")));
  // 2026-09-1 10:00:00 = 2026-09-01 10:00:00 (the 10h is in the seconds).
  EXPECT_EQ(
      Timestamp(1788256800, 0),
      toTimestampOnce(std::string("2026-09-1 10:00:00")));
  EXPECT_EQ(
      Timestamp(1772272800, 0),
      toTimestampOnce(std::string("2026-02-29 10:00:00")));
  // A 19-digit year exceeds the domain: the accumulator bails before any
  // int64 overflow; NULL either way (Flink's parse rejects it too).
  EXPECT_EQ(
      std::nullopt,
      toTimestampOnce(std::string("+9999999999999999999-01-01 00:00:00")));
}

// B. One-arg invalid inputs -> NULL (no trim, full-string consumption,
// mandatory complete HH:mm:ss, EXCEEDS_PAD year rules).
TEST_F(ToTimestampTest, OneArgInvalidToNull) {
  EXPECT_EQ(std::nullopt, toTimestampOnce(std::string("2026-09-11 10:00")));
  EXPECT_EQ(std::nullopt, toTimestampOnce(std::string("2026")));
  EXPECT_EQ(std::nullopt, toTimestampOnce(std::string("2026-01")));
  EXPECT_EQ(std::nullopt, toTimestampOnce(std::string("2026-0")));
  EXPECT_EQ(std::nullopt, toTimestampOnce(std::string("2026--01")));
  EXPECT_EQ(std::nullopt, toTimestampOnce(std::string("2026-13-01 05:00:00")));
  EXPECT_EQ(std::nullopt, toTimestampOnce(std::string("2026-00-01 05:00:00")));
  EXPECT_EQ(std::nullopt, toTimestampOnce(std::string("2026-0-1 05:00:00")));
  EXPECT_EQ(std::nullopt, toTimestampOnce(std::string("2026-09-00 05:00:00")));
  EXPECT_EQ(std::nullopt, toTimestampOnce(std::string("2026-09-32 05:00:00")));
  EXPECT_EQ(std::nullopt, toTimestampOnce(std::string("2026-09-11 23:60:00")));
  EXPECT_EQ(std::nullopt, toTimestampOnce(std::string("2026-09-11 23:59:60")));
  EXPECT_EQ(std::nullopt, toTimestampOnce(std::string("2026-09-11 24:00:01")));
  EXPECT_EQ(std::nullopt, toTimestampOnce(std::string("2026-09-11 24:30:00")));
  EXPECT_EQ(
      std::nullopt, toTimestampOnce(std::string("2026-09-11 24:00:00.5")));
  EXPECT_EQ(std::nullopt, toTimestampOnce(std::string("10000-01-01 00:00:00")));
  EXPECT_EQ(std::nullopt, toTimestampOnce(std::string("09999-01-01 00:00:00")));
  EXPECT_EQ(std::nullopt, toTimestampOnce(std::string("+1000-01-01 00:00:00")));
  EXPECT_EQ(std::nullopt, toTimestampOnce(std::string("-0001-01-01 00:00:00")));
  EXPECT_EQ(std::nullopt, toTimestampOnce(std::string("0000-01-01 00:00:00")));
  EXPECT_EQ(
      std::nullopt, toTimestampOnce(std::string(" 2026-09-11 10:00:00 ")));
  EXPECT_EQ(std::nullopt, toTimestampOnce(std::string("2026-09-11 10:00:00 ")));
  EXPECT_EQ(std::nullopt, toTimestampOnce(std::string("2026-09-11\t10:00:00")));
  EXPECT_EQ(
      std::nullopt, toTimestampOnce(std::string("2026-09-11 10:00:00xyz")));
  EXPECT_EQ(std::nullopt, toTimestampOnce(std::string("2026-09-11.5")));
  EXPECT_EQ(
      std::nullopt,
      toTimestampOnce(std::string("2026-09-11 10:00:00.1234567890")));
  EXPECT_EQ(std::nullopt, toTimestampOnce(std::string("")));
  EXPECT_EQ(std::nullopt, toTimestampOnce(std::string("abc")));
  EXPECT_EQ(std::nullopt, toTimestampOnce(std::string("999-01-01 00:00:00")));
  EXPECT_EQ(std::nullopt, toTimestampOnce(std::string("26-01-01 00:00:00")));
}

// C. One-arg [XX][X] ladder and missing-segment combinations (probed).
TEST_F(ToTimestampTest, OneArgLadderAndDefaults) {
  EXPECT_EQ(
      Timestamp(1768089600, 0), toTimestampOnce(std::string("2026-011-11")));
  EXPECT_EQ(std::nullopt, toTimestampOnce(std::string("2026-019-11")));
  EXPECT_EQ(std::nullopt, toTimestampOnce(std::string("2026-123-11")));
  EXPECT_EQ(
      Timestamp(1793491200, 0), toTimestampOnce(std::string("2026-11-011")));
  EXPECT_EQ(std::nullopt, toTimestampOnce(std::string("2026-09-013")));
  EXPECT_EQ(std::nullopt, toTimestampOnce(std::string("2026-09-123")));
  EXPECT_EQ(
      Timestamp(1789120800, 0),
      toTimestampOnce(std::string("2026-09-11 10:000:00")));
  EXPECT_EQ(
      Timestamp(1789120800, 0),
      toTimestampOnce(std::string("2026-09-11 10:00:000")));
  EXPECT_EQ(std::nullopt, toTimestampOnce(std::string("2026-09-11 10:00:001")));
  EXPECT_EQ(std::nullopt, toTimestampOnce(std::string("2026-09-11 10:001:00")));
  EXPECT_EQ(std::nullopt, toTimestampOnce(std::string("2026-09-11 010:00:00")));
  EXPECT_EQ(std::nullopt, toTimestampOnce(std::string("2026-09-11 100:00:00")));
  EXPECT_EQ(std::nullopt, toTimestampOnce(std::string("2026-09-11 001:00:00")));
  EXPECT_EQ(Timestamp(1767225600, 0), toTimestampOnce(std::string("2026--")));
  EXPECT_EQ(
      Timestamp(1788256800, 0),
      toTimestampOnce(std::string("2026-09- 10:00:00")));
  EXPECT_EQ(
      Timestamp(1767261600, 0),
      toTimestampOnce(std::string("2026-- 10:00:00")));
  EXPECT_EQ(
      Timestamp(1772323200, 0),
      toTimestampOnce(std::string("2026-02-30 24:00:00")));
  EXPECT_EQ(
      Timestamp(253402300800, 0),
      toTimestampOnce(std::string("9999-12-31 24:00:00")));
  EXPECT_EQ(
      Timestamp(1789171200, 0),
      toTimestampOnce(std::string("2026-09-11 24:00:00.000000000")));
  EXPECT_EQ(
      Timestamp(1789171200, 0),
      toTimestampOnce(std::string("2026-09-11 24:00:00.0")));
}

// D. One-arg huge years: GFV range-checks to NULL where Flink wraps (D1).
TEST_F(ToTimestampTest, OneArgHugeYears) {
  EXPECT_EQ(
      Timestamp(9223372036854775, 0),
      toTimestampOnce(std::string("+292278994-08-17 07:12:55")));
  EXPECT_EQ(
      std::nullopt, toTimestampOnce(std::string("+292278995-01-01 00:00:00")));
  EXPECT_EQ(
      std::nullopt, toTimestampOnce(std::string("+999999999-01-01 00:00:00")));
  EXPECT_EQ(
      std::nullopt, toTimestampOnce(std::string("+1000000000-01-01 00:00:00")));
  EXPECT_EQ(
      std::nullopt,
      toTimestampOnce(std::string("+12345678901234567890-01-01 00:00:00")));
}

// E. NULL propagation (framework default; Flink codegen short-circuit).
TEST_F(ToTimestampTest, NullPropagation) {
  EXPECT_EQ(std::nullopt, toTimestampOnce(std::nullopt));
  EXPECT_EQ(std::nullopt, toTimestampOnce(std::nullopt, std::string(kIso)));
  EXPECT_EQ(
      std::nullopt, toTimestampOnce(std::string("2026-09-11"), std::nullopt));
}

// F. Two-arg agreement set (both engines probed to the same value).
TEST_F(ToTimestampTest, TwoArgBasics) {
  EXPECT_EQ(
      Timestamp(1789120800, 0),
      toTimestampOnce(std::string("2026-09-11 10:00:00"), std::string(kFull)));
  EXPECT_EQ(
      Timestamp(1789120800, 0),
      toTimestampOnce(
          std::string("2026/09/11 10:00:00"),
          std::string("yyyy/MM/dd HH:mm:ss")));
  EXPECT_EQ(
      Timestamp(1789084800, 0),
      toTimestampOnce(std::string("2026-09-11"), std::string(kIso)));
  EXPECT_EQ(
      Timestamp(1789120800, 123000000),
      toTimestampOnce(
          std::string("2026-09-11 10:00:00.123"),
          std::string("yyyy-MM-dd HH:mm:ss.SSS")));
  EXPECT_EQ(
      Timestamp(36000, 0),
      toTimestampOnce(std::string("10:00:00"), std::string("HH:mm:ss")));
  EXPECT_EQ(
      Timestamp(1788220800, 0),
      toTimestampOnce(std::string("2026-9-1"), std::string(kIso)));
  EXPECT_EQ(
      Timestamp(-12220243200, 0),
      toTimestampOnce(std::string("1582-10-04"), std::string(kIso)));
  EXPECT_EQ(
      Timestamp(-12219379200, 0),
      toTimestampOnce(std::string("1582-10-14"), std::string(kIso)));
  // The parsed offset is ignored: the wall clock is kept (delta-1, Flink same).
  EXPECT_EQ(
      Timestamp(1789120800, 0),
      toTimestampOnce(
          std::string("2026-09-11 10:00:00+0800"),
          std::string("yyyy-MM-dd HH:mm:ssZ")));
  // Constant format: representative rows pin the compile-once path across
  // format shapes (full datetime, 'S' fraction, pure time, date only); the
  // per-row path above already covers the shared core on every row.
  EXPECT_EQ(
      Timestamp(1789120800, 0),
      toTimestampConstFormatOnce(
          std::string("2026-09-11 10:00:00"), std::string(kFull)));
  EXPECT_EQ(
      Timestamp(1789120800, 123000000),
      toTimestampConstFormatOnce(
          std::string("2026-09-11 10:00:00.123"),
          std::string("yyyy-MM-dd HH:mm:ss.SSS")));
  EXPECT_EQ(
      Timestamp(36000, 0),
      toTimestampConstFormatOnce(
          std::string("10:00:00"), std::string("HH:mm:ss")));
  EXPECT_EQ(
      Timestamp(1789084800, 0),
      toTimestampConstFormatOnce(std::string("2026-09-11"), std::string(kIso)));
}

// G. Two-arg deviations pinned to GFV semantics (T-table).
TEST_F(ToTimestampTest, TwoArgDeviationsPinned) {
  // T2: engine rejects where Flink SMART-clamps.
  EXPECT_EQ(
      std::nullopt,
      toTimestampOnce(std::string("2026-02-30 10:00:00"), std::string(kFull)));
  EXPECT_EQ(
      std::nullopt,
      toTimestampOnce(std::string("2024-02-30 10:00:00"), std::string(kFull)));
  EXPECT_EQ(
      std::nullopt,
      toTimestampOnce(std::string("2026-04-31 10:00:00"), std::string(kFull)));
  // Agreement row: month 13 is NULL on both stacks.
  EXPECT_EQ(
      std::nullopt,
      toTimestampOnce(std::string("2026-13-01 10:00:00"), std::string(kFull)));
  // T3: a parsed date field lazily defaults the year to 2000 (Flink: 1970).
  EXPECT_EQ(
      Timestamp(968630400, 0),
      toTimestampOnce(std::string("09-11"), std::string("MM-dd")));
  // T1: no fallback chain (Flink's Timestamp.valueOf leniently carries
  // 02-30 -> 03-02).
  EXPECT_EQ(
      std::nullopt,
      toTimestampOnce(
          std::string("2026-02-30 10:00:00"), std::string("HH:mm:ss")));
  // T1+T5: 'QQ' is a legal java.time letter saved by Flink's fallback.
  EXPECT_EQ(
      std::nullopt,
      toTimestampOnce(std::string("2026-09-11 10:00:00"), std::string("QQ")));
  // T1: empty format falls back to a value on Flink.
  EXPECT_EQ(
      std::nullopt,
      toTimestampOnce(std::string("2026-09-11 10:00:00"), std::string("")));
  // T1: format/input mismatch rescued by Flink's fallback. The engine takes
  // the '1999-9-10' prefix (variable-width month) and tolerates the trailing
  // ' 05:20:10' (the T10 exemption) — a T1+T10 composite pinned to the GFV
  // value (Flink: 1999-09-10 05:20:10).
  EXPECT_EQ(
      Timestamp(936921600, 0),
      toTimestampOnce(std::string("1999-9-10 05:20:10"), std::string(kIso)));
  EXPECT_EQ(
      std::nullopt,
      toTimestampOnce(std::string("1999-9-10"), std::string(kFull)));
  // T10 GFV-success class: trailing garbage is tolerated by the engine
  // (Flink: NULL after the fallback rejects).
  EXPECT_EQ(
      Timestamp(1789084800, 0),
      toTimestampOnce(std::string("2026-09-11xyz"), std::string(kIso)));
  // T11: no trim on either side (Flink's fallback trims to a value).
  EXPECT_EQ(
      std::nullopt,
      toTimestampOnce(
          std::string(" 2026-09-11 10:00:00 "), std::string(kFull)));
  // T6: 'S' reads count digits as milliseconds (Flink: fraction of a second).
  EXPECT_EQ(
      Timestamp(1789120800, 5000000),
      toTimestampOnce(
          std::string("2026-09-11 10:00:00.5"),
          std::string("yyyy-MM-dd HH:mm:ss.S")));
  EXPECT_EQ(
      Timestamp(1789084812, 345000000),
      toTimestampOnce(
          std::string("2026-09-11 00:00:00.12345"),
          std::string("yyyy-MM-dd HH:mm:ss.SSSSS")));
  // T12: text is case-sensitive (Flink: case-insensitive).
  EXPECT_EQ(
      std::nullopt,
      toTimestampOnce(std::string("2026-jAn-11"), std::string("yyyy-MMM-dd")));
  // T8 agreement band: '00-'69' -> 20xx on both stacks (fixed bases agree).
  EXPECT_EQ(
      Timestamp(2366841600, 0),
      toTimestampOnce(std::string("45-01-01"), std::string("yy-MM-dd")));
  // T8 deviation band: '70-'99' -> 19xx here (Flink: fixed base 2000 -> 20xx).
  EXPECT_EQ(
      Timestamp(0, 0),
      toTimestampOnce(std::string("70-01-01"), std::string("yy-MM-dd")));
  // T13: non-ASCII digits are rejected (Flink's fallback rescues them).
  EXPECT_EQ(
      std::nullopt,
      toTimestampOnce(
          std::string("２０２６-09-11 10:00:00"), std::string(kFull)));
  // T6 negative wrap: the int32-microsecond overflow yields an illegal nanos
  // -> NULL under the guarded parse (both build modes).
  EXPECT_EQ(
      std::nullopt,
      toTimestampOnce(
          std::string("2026-09-11 00:00:00.12345678"),
          std::string("yyyy-MM-dd HH:mm:ss.SSSSSSSS")));
  // T6 positive wrap: a silent deterministic wrong value (pinned).
  EXPECT_EQ(
      Timestamp(1789086210, 64000000),
      toTimestampOnce(
          std::string("2026-09-11 00:00:00.9999999"),
          std::string("yyyy-MM-dd HH:mm:ss.SSSSSSS")));
  // T7 sign form: the engine's 'y' accepts '+2026' (Flink: EXCEEDS_PAD
  // rejects).
  EXPECT_EQ(
      Timestamp(1789084800, 0),
      toTimestampOnce(std::string("+2026-09-11"), std::string(kIso)));
  // G22 seconds-domain big year (pinned per build mode): the debug build
  // throws inside parse() and the catch turns it into NULL; the release
  // semantics would be Timestamp(9223372048665599, 0) (Flink: NULL).
  EXPECT_EQ(
      std::nullopt,
      toTimestampOnce(
          std::string("292278994-12-31 23:59:59"), std::string(kFull)));
}

// H. Two-arg illegal format -> NULL on both paths (delta-2; the opposite of
// to_date's job-failure disposition — Flink's ofPattern IAE is try-caught).
TEST_F(ToTimestampTest, TwoArgIllegalFormatToNull) {
  EXPECT_EQ(
      std::nullopt,
      toTimestampOnce(std::string("2026-09-11 10:00:00"), std::string("RR")));
  EXPECT_EQ(
      std::nullopt,
      toTimestampOnce(std::string("2026-09-11 10:00:00"), std::string("abc")));
  EXPECT_EQ(
      std::nullopt,
      toTimestampConstFormatOnce(
          std::string("2026-09-11 10:00:00"), std::string("RR")));
  EXPECT_EQ(
      std::nullopt,
      toTimestampConstFormatOnce(
          std::string("2026-09-11 10:00:00"), std::string("abc")));
  EXPECT_EQ(
      std::nullopt,
      toTimestampOnce(std::string("2026-09-11 10:00:00"), std::string("'abc")));
  EXPECT_EQ(
      std::nullopt,
      toTimestampConstFormatOnce(
          std::string("2026-09-11 10:00:00"), std::string("'abc")));
}

// I. Wrapper structure: mixed column formats, constant-format compile path,
// and a bad row among good rows must not fail the batch.
TEST_F(ToTimestampTest, TwoArgColumnFormatMultiRow) {
  auto data = makeRowVector(std::vector<VectorPtr>{
      makeNullableFlatVector<std::string>(
          {"2026-09-11 10:00:00", "2026/09/11 10:00:00", "2026-09-11"}),
      makeNullableFlatVector<std::string>(
          {"yyyy-MM-dd HH:mm:ss", "yyyy/MM/dd HH:mm:ss", "yyyy-MM-dd"})});
  auto result = evaluate("to_timestamp(c0, c1)", data);
  ASSERT_EQ(3, result->size());
  auto flat = result->asFlatVector<Timestamp>();
  EXPECT_EQ(Timestamp(1789120800, 0), flat->valueAt(0));
  EXPECT_EQ(Timestamp(1789120800, 0), flat->valueAt(1));
  EXPECT_EQ(Timestamp(1789084800, 0), flat->valueAt(2));

  // A bad format row is a NULL row; the job does not fail (I3, Flink same).
  auto mixed = makeRowVector(std::vector<VectorPtr>{
      makeNullableFlatVector<std::string>({"2026-09-11", "2026-09-11"}),
      makeNullableFlatVector<std::string>({"yyyy-MM-dd", "RR"})});
  auto mixedResult = evaluate("to_timestamp(c0, c1)", mixed);
  ASSERT_EQ(2, mixedResult->size());
  auto mixedFlat = mixedResult->asFlatVector<Timestamp>();
  EXPECT_EQ(Timestamp(1789084800, 0), mixedFlat->valueAt(0));
  EXPECT_TRUE(mixedResult->isNullAt(1));
}

} // namespace facebook::velox::functions::flinksql::test
