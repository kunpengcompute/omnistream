/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

// GroupTableAggregateFunctionTest.cpp
//
// Unit tests for the native GroupTableAggregate operator (Top2 table aggregate).
// Mirrors the GroupAggFunctionTest setup pattern: the operator is created through
// StreamOperatorFactory from a JSON description (so the test also covers the factory
// registration branch), then rows are fed via setKeyContextElement + processElement.
//
// State model under test (Scheme A, Flink state-shape parity): all persistent state
// lives in the operator's ValueState<RowData> accState as a [first, second] row of
// rankEnd BIGINT slots -- the same shape plain Flink's codegen'd Top2 handler
// persists, so checkpoints are engine-interoperable. No MapView/ledger exists.
//
// Input schema (3 BIGINT columns): [category, bidder, price]
//   grouping=[0] (category), sortFieldIndices=[2] price DESC, keyless (no
//   uniqueKeyIndex -- the Top2 acceptance shape), rankRange=Top-2.
//   Output rows: [category, price, rn] with changelog RowKind.
//
// Case mapping to the test-design document:
//   1 MultiRowOutputCorrectness       (case 1: multi-row output, ordering)
//   2 FirstRowPath                    (case 2: createAccumulators branch)
//   3 UpdateBeforeOrdering            (case 3: -U rows strictly before +U rows)
//   4 NoUpdateBefore                  (case 4: negative -- no -U row at all)
//   5 RetractInputIsDropped           (case 5 reworked: the Top2 UDF declares no
//                                      retract, so -D input is dropped silently and
//                                      the next +I row recovers normally)
//   6 DualBackendConsistency          (cases 6+7: HashMap vs RocksDB identical output;
//                                      RocksDB exercises the accState RowData
//                                      serialize/deserialize round-trip)
//   7 AccumulatorPersistsAcrossKeys   (case 8 reworked: after a key switch the
//                                      group's [first, second] is restored from
//                                      accState -- proves Scheme A persistence)
//   8 RetractForUnknownGroupIsDropped (bonus guard: no output, no state creation)
//   9 NotInTopEmitsUnchangedBoard     (bonus: a value below the board still
//                                      re-emits the whole board -- Flink emits
//                                      emitValue output on EVERY input row)
//  10 NoUniqueKeyMatchesFlinkTop2     (keyless acceptance shape, 12-row changelog)
//  11 NoUniqueKeyDuplicateValueRanksTwice (same price twice ranks twice: plain
//                                      Flink Top2 TableAggregateFunction semantics)

#include "table/runtime/operators/aggregate/GroupTableAggregateFunction.h"
#include <nlohmann/json.hpp>
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "streaming/api/operators/StreamOperatorFactory.h"
#include <gtest/gtest.h>
#include "table/data/binary/BinaryRowData.h"
#include "table/data/RowData.h"
#include "table/data/RowKind.h"
#include "runtime/taskmanager/OmniRuntimeEnvironment.h"
#include "runtime/state/EmbeddedRocksDBStateBackend.h"
#include "core/api/common/TaskInfoImpl.h"
#include "table/typeutils/RowDataSerializer.h"
#include "streaming/api/operators/KeyedProcessOperator.h"
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

using namespace omnistream;
using namespace omnistream::datastream;

namespace {

// One captured output row: values are copied while the row is still alive.
struct CapturedRow {
    RowKind kind;
    long category; // field 0 of the emitted [key | price, rn] row
    long price;    // field 1
    long rn;       // field 2
};

std::string rowToString(const CapturedRow& r)
{
    return std::string(to_string(r.kind)) + "|" + std::to_string(r.category) + "|" + std::to_string(r.price) + "|"
        + std::to_string(r.rn);
}

// Captures everything the operator emits. TimestampedCollector wraps each emitted
// RowData into a fresh StreamRecord; we unwrap, copy out the values, and free the
// wrapper (the StreamRecord does not own its value).
class CapturingOutput : public WatermarkGaugeExposingOutput {
public:
    void collect(void* record) override
    {
        auto* streamRecord = reinterpret_cast<StreamRecord*>(record);
        auto* row = static_cast<RowData*>(streamRecord->getValue());
        CapturedRow c;
        c.kind = row->getRowKind();
        c.category = row->getLong(0) != nullptr ? *(row->getLong(0)) : -1;
        c.price = row->getLong(1) != nullptr ? *(row->getLong(1)) : -1;
        c.rn = row->getLong(2) != nullptr ? *(row->getLong(2)) : -1;
        captured.push_back(c);
        delete streamRecord;
    }

    void close() override {}

    void emitWatermark(Watermark* watermark) override {}

    void emitWatermarkStatus(WatermarkStatus* watermarkStatus) override {}

    std::vector<CapturedRow> captured;
};

// Test rig: operator + capturing output + fed input records.
// Input rows are kept alive for the whole test (the operator may reference them),
// mirroring GroupAggFunctionTest's cleanup-at-end style.
struct GroupTableAggTestRig {
    KeyedProcessOperator<RowData*, RowData*, RowData*>* op = nullptr;
    CapturingOutput* output = nullptr;
    std::vector<StreamRecord*> fedRecords;

    ~GroupTableAggTestRig()
    {
        for (auto* record : fedRecords) {
            delete record;
        }
        // op / output / env are intentionally leaked, matching the suite's style.
    }

    // One bid row: [category, bidder, price] with an optional changelog kind.
    void feed(long category, long bidder, long price, RowKind kind = RowKind::INSERT)
    {
        auto* row = BinaryRowData::createBinaryRowDataWithMem(3);
        row->setLong(0, category);
        row->setLong(1, bidder);
        row->setLong(2, price);
        row->setRowKind(kind);
        auto* record = new StreamRecord(row);
        fedRecords.push_back(record);
        op->setKeyContextElement(record);
        op->processElement(record);
    }

    std::vector<std::string> capturedStrings() const
    {
        std::vector<std::string> out;
        out.reserve(output->captured.size());
        for (const auto& c : output->captured) {
            out.push_back(rowToString(c));
        }
        return out;
    }

    void clearCaptured()
    {
        output->captured.clear();
    }
};

// Keyless rig (the Top2 acceptance shape: no uniqueKeyIndex in the JSON). The
// accTypes array mirrors the real Flink plan: the Top2 accumulator row is
// [first BIGINT, second BIGINT].
GroupTableAggTestRig makeRig(bool generateUpdateBefore, const std::string& backend)
{
    std::string description = R"DELIM({
        "input_channels": [0],
        "operators": [{
            "description": {
                "processFunction": "GroupTableAggregateFunction",
                "grouping": [0],
                "inputTypes": ["BIGINT", "BIGINT", "BIGINT"],
                "outputTypes": ["BIGINT", "BIGINT", "BIGINT"],
                "sortFieldIndices": [2],
                "sortAscendingOrders": [false],
                "rankRange": "rankStart=1, rankEnd=2",
                "generateUpdateBefore": true,
                "aggInfoList": {
                    "accTypes": ["BIGINT", "BIGINT"],
                    "aggValueTypes": ["BIGINT", "BIGINT"],
                    "aggregateCalls": [{"aggregationFunction": "ROW_NUMBER", "argIndexes": [2],
                                        "name": "ROW_NUMBER($2)", "filterArg": -1}],
                    "indexOfCountStar": -1
                }
            },
            "id": "org.apache.flink.streaming.api.operators.KeyedProcessOperator",
            "inputs": [{"kind": "Row", "type": [
                {"isNull": true, "kind": "logical", "type": "BIGINT"},
                {"isNull": true, "kind": "logical", "type": "BIGINT"},
                {"isNull": true, "kind": "logical", "type": "BIGINT"}]}],
            "name": "GroupTableAggregate[0]",
            "output": {"kind": "Row", "type": [
                {"isNull": true, "kind": "logical", "type": "BIGINT"},
                {"isNull": true, "kind": "logical", "type": "BIGINT"},
                {"isNull": true, "kind": "logical", "type": "BIGINT"}]}
        }],
        "partition": {"channelNumber": 1, "partitionName": "forward"}
    })DELIM";

    json parsedJson = json::parse(description);
    parsedJson["operators"][0]["description"]["generateUpdateBefore"] = generateUpdateBefore;

    omnistream::OperatorConfig opConfig(
        "org.apache.flink.streaming.api.operators.KeyedProcessOperator",
        "Group_Table_Agg",
        parsedJson["operators"][0]["inputTypes"],
        parsedJson["operators"][0]["outputTypes"],
        parsedJson["operators"][0]["description"]);

    auto* output = new CapturingOutput();
    StreamOperatorFactory streamOperatorFactory;
    GroupTableAggTestRig rig;
    rig.op = dynamic_cast<KeyedProcessOperator<RowData*, RowData*, RowData*>*>(
        streamOperatorFactory.createOperatorAndCollector(opConfig, output));
    rig.output = output;
    // If this fails the JSON above no longer reaches the GroupTableAggregateFunction
    // factory branch (e.g. processFunction string mismatch).
    EXPECT_NE(rig.op, nullptr);

    auto* env2 = new omnistream::RuntimeEnvironmentV2();
    auto* taskInfo = new TaskInformationPOD();
    taskInfo->setStateBackend(backend);
    const bool isRocksDb = (backend == "EmbeddedRocksDBStateBackend");
    if (isRocksDb) {
        // RocksDB needs an explicit scratch directory (the RowTimeDeduplicateFunctionTest
        // RocksDB cases follow the same pattern).
        const auto rocksPath = std::filesystem::temp_directory_path() / "group-table-agg-rocksdb";
        std::filesystem::remove_all(rocksPath);
        taskInfo->setRocksdbStorePaths({rocksPath.string()});
    }
    {
        auto configPOD = taskInfo->getStreamConfigPOD();
        auto operatorDesc = configPOD.getOperatorDescription();
        operatorDesc.setOperatorId("deadbeefdeadbeefdeadbeefdeadbeef");
        configPOD.setOperatorDescription(operatorDesc);
        taskInfo->setStreamConfigPOD(configPOD);
    }
    if (isRocksDb) {
        // The RocksDB restore path needs the full TaskStateManager (job/attempt ids).
        auto jobId = omnistream::JobIDPOD(1, 2);
        auto jobVertexId = omnistream::JobVertexID(3, 4);
        auto attemptId = omnistream::ExecutionAttemptIDPOD::randomId();
        auto* stateStore = new TaskLocalStateStore(jobId, jobVertexId, 0, nullptr);
        auto* responder = new omnistream::NoOpCheckpoingResponder();
        env2->SetTaskStateManager(
            std::make_shared<omnistream::TaskStateManager>(jobId, attemptId, stateStore, responder));
    } else {
        env2->SetTaskStateManager(std::make_shared<omnistream::TaskStateManager>());
    }
    env2->setTaskConfiguration(*taskInfo);
    // keyedStatedBackend() dynamic_casts the initializer's backend to
    // EmbeddedRocksDBStateBackend, so the RocksDB path must pass a real backend instance
    // (two-arg constructor); the heap path uses the single-arg initializer.
    StreamTaskStateInitializerImpl* initializer = isRocksDb
        ? new StreamTaskStateInitializerImpl(new EmbeddedRocksDBStateBackend(*taskInfo), env2)
        : new StreamTaskStateInitializerImpl(env2);
    TypeSerializer* ser = nullptr;
    if (isRocksDb) {
        // RocksDB really serializes keys with this serializer: use the grouping schema
        // (1 BIGINT key column), same as the Deduplicate RocksDB tests.
        auto* keyFields =
            new std::vector<omnistream::RowField>({omnistream::RowField("category", BasicLogicalType::BIGINT)});
        ser = new RowDataSerializer(new omnistream::RowType(false, *keyFields));
    } else {
        auto* typeInfo = new std::vector<omnistream::RowField>(
            {omnistream::RowField("category", BasicLogicalType::BIGINT),
             omnistream::RowField("bidder", BasicLogicalType::BIGINT),
             omnistream::RowField("price", BasicLogicalType::BIGINT)});
        ser = new RowDataSerializer(new omnistream::RowType(false, *typeInfo));
    }

    rig.op->initializeState(initializer, ser);
    rig.op->open();
    return rig;
}

void expectOutput(const CapturingOutput& out, const std::vector<std::string>& expected, const char* what)
{
    ASSERT_EQ(out.captured.size(), expected.size()) << what << ": row count mismatch";
    for (size_t i = 0; i < expected.size() && i < out.captured.size(); ++i) {
        EXPECT_EQ(rowToString(out.captured[i]), expected[i]) << what << ": row[" << i << "] mismatch";
    }
}

} // namespace

// ---------------------------------------------------------------------------
// Case 1: multi-row output correctness. Four bids in one group; each arriving row
// retracts the old board (when full) and emits the new board. Final board must be
// 赵六 9000 rank1 / 李四 8000 rank2 (price DESC).
// ---------------------------------------------------------------------------
TEST(GroupTableAggregateFunctionTest, MultiRowOutputCorrectness)
{
    auto rig = makeRig(true, "HashMapStateBackend");

    rig.feed(1, /*bidder=*/1, /*price=*/5000); // 张三
    rig.feed(1, 2, 8000);                     // 李四
    rig.feed(1, 3, 6500);                     // 王五 (evicts 张三)
    rig.feed(1, 4, 9000);                     // 赵六 (evicts 王五)

    expectOutput(*rig.output,
        {"+I|1|5000|1",
         "-U|1|5000|1", "+U|1|8000|1", "+U|1|5000|2",
         "-U|1|8000|1", "-U|1|5000|2", "+U|1|8000|1", "+U|1|6500|2",
         "-U|1|8000|1", "-U|1|6500|2", "+U|1|9000|1", "+U|1|8000|2"},
        "case1-multi-row");
}

// ---------------------------------------------------------------------------
// Case 2: first row of a brand-new group walks the createAccumulators branch and
// emits exactly one INSERT row (no retraction against an empty board).
// ---------------------------------------------------------------------------
TEST(GroupTableAggregateFunctionTest, FirstRowPath)
{
    auto rig = makeRig(true, "HashMapStateBackend");

    rig.feed(/*category=*/2, /*bidder=*/5, /*price=*/3000); // 马七, 服装类

    expectOutput(*rig.output, {"+I|2|3000|1"}, "case2-first-row");
}

// ---------------------------------------------------------------------------
// Case 3: with generateUpdateBefore=true, the retraction rows (-U) for the old
// board must be emitted strictly before the new board rows (+U).
// ---------------------------------------------------------------------------
TEST(GroupTableAggregateFunctionTest, UpdateBeforeOrdering)
{
    auto rig = makeRig(true, "HashMapStateBackend");

    rig.feed(1, 1, 5000); // 张三
    rig.feed(1, 2, 8000); // 李四 -> board [李四8000, 张三5000]
    rig.clearCaptured();

    rig.feed(1, 3, 6500); // 王五 evicts 张三

    expectOutput(*rig.output,
        {"-U|1|8000|1", "-U|1|5000|2", // old board first
         "+U|1|8000|1", "+U|1|6500|2"}, // new board after
        "case3-update-before-order");
}

// ---------------------------------------------------------------------------
// Case 4: with generateUpdateBefore=false the same eviction scenario emits no -U
// row at all, while the board content stays correct.
// ---------------------------------------------------------------------------
TEST(GroupTableAggregateFunctionTest, NoUpdateBefore)
{
    auto rig = makeRig(false, "HashMapStateBackend");

    rig.feed(1, 1, 5000);
    rig.feed(1, 2, 8000);
    rig.feed(1, 3, 6500);

    expectOutput(*rig.output,
        {"+I|1|5000|1",
         "+U|1|8000|1", "+U|1|5000|2",
         "+U|1|8000|1", "+U|1|6500|2"},
        "case4-no-update-before");
    // Negative assertion: not a single -U in the whole stream.
    for (const auto& c : rig.output->captured) {
        EXPECT_NE(c.kind, RowKind::UPDATE_BEFORE) << "case4: -U emitted although generateUpdateBefore=false";
    }
}

// ---------------------------------------------------------------------------
// Case 5 (reworked for Top2 semantics): the Top2 UDF declares no retract, so a -D
// input row is dropped silently -- no output, no state change -- and the next +I
// row proceeds normally from the preserved board.
// ---------------------------------------------------------------------------
TEST(GroupTableAggregateFunctionTest, RetractInputIsDropped)
{
    auto rig = makeRig(true, "HashMapStateBackend");

    rig.feed(1, 2, 8000); // 李四
    rig.feed(1, 1, 5000); // 张三 -> board [李四8000, 张三5000]
    rig.clearCaptured();

    rig.feed(1, 2, 8000, RowKind::DELETE); // -D: not supported by the Top2 UDF
    EXPECT_TRUE(rig.output->captured.empty()) << "case5: dropped retract must emit nothing";

    rig.clearCaptured();
    rig.feed(1, 3, 9000); // 赵六: board [9000, 8000] proves the old board survived
    expectOutput(*rig.output,
        {"-U|1|8000|1", "-U|1|5000|2", "+U|1|9000|1", "+U|1|8000|2"},
        "case5-recover-after-retract-drop");
}

// ---------------------------------------------------------------------------
// Cases 6+7: the same data under HashMap and RocksDB backends must produce
// byte-identical changelog streams. RocksDB exercises the real serialize /
// deserialize round-trip of the Scheme A accState accumulator row
// ([first, second] as a ValueState<RowData>) -- engine-interoperable state.
// ---------------------------------------------------------------------------
TEST(GroupTableAggregateFunctionTest, DualBackendConsistency)
{
    const std::vector<std::string> golden = {
        "+I|1|5000|1",
        "-U|1|5000|1", "+U|1|8000|1", "+U|1|5000|2",
        "-U|1|8000|1", "-U|1|5000|2", "+U|1|8000|1", "+U|1|6500|2"};

    auto runScenario = [](const std::string& backend) {
        auto rig = makeRig(true, backend);
        rig.feed(1, 1, 5000);
        rig.feed(1, 2, 8000);
        rig.feed(1, 3, 6500);
        return rig.capturedStrings();
    };

    auto heapOut = runScenario("HashMapStateBackend");
    auto rocksOut = runScenario("EmbeddedRocksDBStateBackend");

    ASSERT_EQ(heapOut, golden);  // not vacuous: pin the HashMap run to the golden sequence
    ASSERT_EQ(rocksOut, heapOut); // then require RocksDB to match exactly
}

// ---------------------------------------------------------------------------
// Case 8 (reworked for Scheme A): switching keys back and forth must restore each
// group's [first, second] from its accState row. category 1 builds a board, then
// category 2 is processed (a key switch that would clobber a per-operator working
// copy), then category 1 continues -- its board must continue from the persisted
// accumulator, not from scratch.
// ---------------------------------------------------------------------------
TEST(GroupTableAggregateFunctionTest, AccumulatorPersistsAcrossKeys)
{
    auto rig = makeRig(true, "HashMapStateBackend");

    rig.feed(1, 1, 5000); // category 1: board [5000]
    rig.feed(1, 2, 8000); // category 1: board [8000, 5000]
    rig.feed(2, 3, 3000); // category 2: board [3000] (key switch in between)
    rig.clearCaptured();

    // Back to category 1: 6500 enters the RESTORED [8000, 5000] board (evicts 5000).
    // If the accumulator had not persisted, the board would be [6500] alone.
    rig.feed(1, 4, 6500);
    expectOutput(*rig.output,
        {"-U|1|8000|1", "-U|1|5000|2", "+U|1|8000|1", "+U|1|6500|2"},
        "case8-accumulator-persists");
}

// ---------------------------------------------------------------------------
// Bonus: a retract message for a group that never existed is dropped silently
// (covered by the unconditional retract drop, and no state must be created).
// ---------------------------------------------------------------------------
TEST(GroupTableAggregateFunctionTest, RetractForUnknownGroupIsDropped)
{
    auto rig = makeRig(true, "HashMapStateBackend");

    rig.feed(1, 1, 5000, RowKind::DELETE); // -D against empty state

    EXPECT_TRUE(rig.output->captured.empty()) << "retract for unknown group must not emit anything";
}

// ---------------------------------------------------------------------------
// Bonus (Top2 semantics): a value that does not make the board still triggers the
// full re-emission flow -- Flink runs emitValue on EVERY input row, so the old
// board is retracted (-U) and the unchanged board is re-emitted (+U). This pins
// the "every input emits, even a no-op" behavior of the acceptance changelog.
// ---------------------------------------------------------------------------
TEST(GroupTableAggregateFunctionTest, NotInTopEmitsUnchangedBoard)
{
    auto rig = makeRig(true, "HashMapStateBackend");

    rig.feed(1, 1, 8000); // board [8000]
    rig.feed(1, 2, 5000); // board [8000, 5000]
    rig.clearCaptured();

    rig.feed(1, 3, 3000); // below the board: no state change, but the board is re-emitted

    expectOutput(*rig.output,
        {"-U|1|8000|1", "-U|1|5000|2", "+U|1|8000|1", "+U|1|5000|2"},
        "bonus-not-in-top-reemits");
}

// ---------------------------------------------------------------------------
// Keyless acceptance shape (uniqueKeyIndex absent, as the Top2 whitelist plan
// writes it): four distinct prices produce the canonical 12-row changelog,
// the exact sequence the acceptance SQL (top2_bid.csv, bidder=1 series) produces.
// ---------------------------------------------------------------------------
TEST(GroupTableAggregateFunctionTest, NoUniqueKeyMatchesFlinkTop2)
{
    auto rig = makeRig(true, "HashMapStateBackend");

    rig.feed(1, 1, 5000); // 张三
    rig.feed(1, 2, 8000); // 李四
    rig.feed(1, 3, 6500); // 王五
    rig.feed(1, 4, 9000); // 赵六

    // Same 12-row changelog as MultiRowOutputCorrectness.
    expectOutput(
        *rig.output,
        {"+I|1|5000|1",
         "-U|1|5000|1", "+U|1|8000|1", "+U|1|5000|2",
         "-U|1|8000|1", "-U|1|5000|2", "+U|1|8000|1", "+U|1|6500|2",
         "-U|1|8000|1", "-U|1|6500|2", "+U|1|9000|1", "+U|1|8000|2"},
        "keyless-top2-changelog");
}

// ---------------------------------------------------------------------------
// Keyless semantics fork: the SAME price arriving twice ranks twice (plain Flink Top2
// TableAggregateFunction semantics -- the equal value settles strictly below the
// occupied slot instead of being deduplicated).
// ---------------------------------------------------------------------------
TEST(GroupTableAggregateFunctionTest, NoUniqueKeyDuplicateValueRanksTwice)
{
    auto rig = makeRig(true, "HashMapStateBackend");

    rig.feed(1, 1, 5000);
    rig.feed(1, 2, 5000); // same price: still ranks twice

    // First row: single-member board (+I). Second row: two equal prices on the board,
    // earlier arrival ranks first (strict '>' keeps the earlier one at rank 1).
    expectOutput(
        *rig.output,
        {"+I|1|5000|1", "-U|1|5000|1", "+U|1|5000|1", "+U|1|5000|2"},
        "keyless-duplicate-price");
}
