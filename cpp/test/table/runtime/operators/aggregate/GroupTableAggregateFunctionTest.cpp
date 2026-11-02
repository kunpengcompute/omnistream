/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You can obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FITNESS FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

// GroupTableAggregateFunctionTest.cpp
//
// Unit tests for the native GroupTableAggregate operator (ROW_NUMBER Top-N leaderboard).
// Mirrors the GroupAggFunctionTest setup pattern: the operator is created through
// StreamOperatorFactory from a JSON description (so the test also covers the factory
// registration branch), then rows are fed via setKeyContextElement + processElement.
//
// Differences vs GroupAggFunctionTest:
//  - GroupAgg asserts on the reused getResultRow(); this operator emits whole boards
//    through TimestampedCollector::collect (0..N rows per input row), so assertions
//    capture what arrives at the downstream output. TimestampedCollector wraps each
//    emitted row into a fresh StreamRecord, hence the capturing output unwraps it.
//  - Field values are copied out inside collect() while the row is guaranteed alive;
//    the operator may replace its key rows later, so capturing RowData* pointers
//    would risk dangling reads.
//
// Input schema (3 BIGINT columns): [category, bidder, price]
//   grouping=[0] (category), sortFieldIndices=[2] price DESC, uniqueKeyIndex=1 (bidder),
//   rankRange=Top-2. Output rows: [category, price, rn] with changelog RowKind.
//
// Case mapping to the test-design document:
//   1 MultiRowOutputCorrectness   (case 1: multi-row output, ordering)
//   2 FirstRowPath                (case 2: createAccumulators branch)
//   3 UpdateBeforeOrdering        (case 3: -U rows strictly before +U rows)
//   4 NoUpdateBefore              (case 4: negative -- no -U row at all)
//   5 RetractAndSilentDeath       (case 5: retract input + silent group death)
//   6 DualBackendConsistency      (cases 6+7: HashMap vs RocksDB identical output)
//   7 DeathAndRebuild             (case 8: state cleared on death -> firstRow rebuild;
//                                  the StateTtlConfig variant is deferred to v2)
//   8 RetractForUnknownGroupIsDropped  (bonus: GroupAggFunction parity guard)
//   9 DuplicateBidIsIdempotent    (bonus: uniqueId ledger, replay does not duplicate)

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

GroupTableAggTestRig makeRig(bool generateUpdateBefore, const std::string& backend, int uniqueKeyIndex = 1)
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
                "uniqueKeyIndex": 1,
                "generateUpdateBefore": true,
                "aggInfoList": {
                    "accTypes": [],
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
    if (uniqueKeyIndex == -1) {
        // Keyless mode: the Java side omits the field entirely when the plan has no identity
        // column (Top2 acceptance SQL); exercise that exact JSON shape here.
        parsedJson["operators"][0]["description"].erase("uniqueKeyIndex");
    } else {
        parsedJson["operators"][0]["description"]["uniqueKeyIndex"] = uniqueKeyIndex;
    }

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
// Case 5: -D input rows walk retract(); when the board empties the group dies
// silently -- the old board was already retracted, and no further row appears.
// ---------------------------------------------------------------------------
TEST(GroupTableAggregateFunctionTest, RetractAndSilentDeath)
{
    auto rig = makeRig(true, "HashMapStateBackend");

    rig.feed(1, 2, 8000); // 李四
    rig.feed(1, 1, 5000); // 张三 -> board [李四8000, 张三5000]
    rig.clearCaptured();

    rig.feed(1, 2, 8000, RowKind::DELETE); // -D 李四: board shrinks to [张三5000]
    expectOutput(*rig.output,
        {"-U|1|8000|1", "-U|1|5000|2", "+U|1|5000|1"},
        "case5-retract-one");

    rig.clearCaptured();
    rig.feed(1, 1, 5000, RowKind::DELETE); // -D 张三: board empties -> group death
    // Silent death: exactly the pre-death retraction, and nothing after it.
    expectOutput(*rig.output, {"-U|1|5000|1"}, "case5-silent-death");
}

// ---------------------------------------------------------------------------
// Cases 6+7: the same data under HashMap and RocksDB backends must produce
// byte-identical changelog streams. RocksDB exercises the serialize/deserialize
// path of the zero-slot accState and the MapView ledger.
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
// Case 8 (TTL deferred to v2, replaced by programmatic death): after the group
// dies, the same key arriving again must walk the firstRow path and emit an
// INSERT (proving accState was cleared), not an UPDATE_AFTER.
// ---------------------------------------------------------------------------
TEST(GroupTableAggregateFunctionTest, DeathAndRebuild)
{
    auto rig = makeRig(true, "HashMapStateBackend");

    rig.feed(1, 2, 8000);                        // +I 李四
    rig.clearCaptured();
    rig.feed(1, 2, 8000, RowKind::DELETE);        // -D 李四 -> death (only the retraction row)
    ASSERT_EQ(rig.output->captured.size(), 1u);

    rig.clearCaptured();
    rig.feed(1, 3, 6500);                         // same key, brand-new group
    expectOutput(*rig.output, {"+I|1|6500|1"}, "case8-rebuild");
}

// ---------------------------------------------------------------------------
// Bonus: a retract message for a group that never existed is dropped silently
// (same policy as GroupAggFunction).
// ---------------------------------------------------------------------------
TEST(GroupTableAggregateFunctionTest, RetractForUnknownGroupIsDropped)
{
    auto rig = makeRig(true, "HashMapStateBackend");

    rig.feed(1, 1, 5000, RowKind::DELETE); // -D against empty state

    EXPECT_TRUE(rig.output->captured.empty()) << "retract for unknown group must not emit anything";
}

// ---------------------------------------------------------------------------
// Bonus: replaying the same bid (unique bidder + price) is idempotent -- the
// uniqueId ledger keeps a single member; the board is not duplicated.
// ---------------------------------------------------------------------------
TEST(GroupTableAggregateFunctionTest, DuplicateBidIsIdempotent)
{
    auto rig = makeRig(true, "HashMapStateBackend");

    rig.feed(1, 1, 5000); // 张三 5000
    rig.clearCaptured();

    rig.feed(1, 1, 5000); // same bid again

    // One member on the board: exactly one retraction and one new-value row.
    expectOutput(*rig.output, {"-U|1|5000|1", "+U|1|5000|1"}, "bonus-duplicate-idempotent");
}

// ---------------------------------------------------------------------------
// Keyless mode (uniqueKeyIndex missing): the acceptance SQL shape. The input row of the
// GroupTableAggregate is [grouping key, UDF argument] with no identity column, so the
// ledger key degenerates to the insertion sequence and idempotence is off. With distinct
// prices the changelog must be byte-identical to the keyed mode (idempotence never fires).
TEST(GroupTableAggregateFunctionTest, NoUniqueKeyMatchesFlinkTop2)
{
    auto rig = makeRig(true, "HashMapStateBackend", -1);

    rig.feed(1, 1, 5000); // 张三
    rig.feed(1, 2, 8000); // 李四
    rig.feed(1, 3, 6500); // 王五
    rig.feed(1, 4, 9000); // 赵六

    // Same 12-row changelog as MultiRowOutputCorrectness (keyed mode): the board content
    // only depends on (price, arrival order), both identical here.
    expectOutput(
        *rig.output,
        {"+I|1|5000|1",
         "-U|1|5000|1", "+U|1|8000|1", "+U|1|5000|2",
         "-U|1|8000|1", "-U|1|5000|2", "+U|1|8000|1", "+U|1|6500|2",
         "-U|1|8000|1", "-U|1|6500|2", "+U|1|9000|1", "+U|1|8000|2"},
        "keyless-top2-changelog");
}

// ---------------------------------------------------------------------------
// Keyless mode semantics fork: the SAME price arriving twice ranks twice (plain Flink Top2
// TableAggregateFunction semantics -- accumulate() lets an equal value take the second
// place). The keyed mode would instead drop the second row as a duplicate identity.
TEST(GroupTableAggregateFunctionTest, NoUniqueKeyDuplicateValueRanksTwice)
{
    auto rig = makeRig(true, "HashMapStateBackend", -1);

    rig.feed(1, 1, 5000);
    rig.feed(1, 2, 5000); // same price, different bidder: still ranks twice

    // First row: single-member board (+I). Second row: two equal prices on the board,
    // earlier arrival ranks first (insertion-order tie-break = seqNo ascending).
    expectOutput(
        *rig.output,
        {"+I|1|5000|1", "-U|1|5000|1", "+U|1|5000|1", "+U|1|5000|2"},
        "keyless-duplicate-price");
}
