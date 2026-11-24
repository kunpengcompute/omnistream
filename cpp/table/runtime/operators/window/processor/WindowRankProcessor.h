/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#pragma once

#include <memory>
#include <vector>

// Include order matters: SlicingWindowOperator.h pulls AbstractWindowAggProcessor.h
// which pulls SlicingWindowProcessor.h. Entering through SlicingWindowOperator.h
// first (like every sibling processor TU does) breaks that include cycle.
#include "table/runtime/operators/window/SlicingWindowOperator.h"
#include "SlicingWindowProcessor.h"
#include "table/runtime/keyselector/KeySelector.h"
#include "table/runtime/operators/window/state/WindowMapState.h"
#include "streaming/api/operators/TimestampedCollector.h"
#include "table/typeutils/BinaryRowDataListSerializer.h"
#include "core/typeutils/LongSerializer.h"

/**
 * Native port of Flink's
 * org.apache.flink.table.runtime.operators.rank.window.processors.WindowRankProcessor
 * (window TVF Top-N). Driven by SlicingWindowOperator, same as the Java
 * processor driven by the Java SlicingWindowOperator.
 *
 * State layout mirrors Flink exactly:
 *   - state name "window_rank"
 *   - MapState<RowData sortKey, List<RowData>> keyed by the partition key,
 *     namespaced by the window end (LongSerializer), i.e. the rows themselves
 *     are stored in the state backend (BinaryRowData copies), NOT combo ids.
 *
 * Differences from Flink (behaviour-equivalent simplifications):
 *   - Flink buffers rows in a RecordsWindowBuffer and combines them into the
 *     state on flush (with a TopN pre-trim optimization). The native processor
 *     writes each row into the state directly during processBatch; the output
 *     of fireWindow is identical because the rank range is applied on the full
 *     window contents either way.
 *   - Flink registers the window timer from TopNRecordsCombiner.combine (on
 *     buffer flush); the native processor registers it in processBatch. The
 *     timer service deduplicates, so the trigger behaviour is identical.
 */
class WindowRankProcessor : public SlicingWindowProcessor<std::shared_ptr<RowData>, int64_t> {
public:
    using KeyType = std::shared_ptr<RowData>;
    using UK = RowData*;                       // sort key row (BinaryRowData)
    using UV = std::vector<RowData*>*;         // list of buffered rows
    using StateMap = emhash7::HashMap<UK, UV>; // native counterpart of Flink's TreeMap content

    WindowRankProcessor(const nlohmann::json& description, Output* output);
    ~WindowRankProcessor() = default;

    void open(
        AbstractKeyedStateBackend<KeyType>* state,
        const nlohmann::json& config,
        StreamingRuntimeContext<KeyType>* runtimeCtx,
        InternalTimerServiceImpl<KeyType, int64_t>* internalTimerService) override;
    void initializeWatermark(int64_t watermark) override;
    bool processBatch(omnistream::VectorBatch* vectorbatch) override;
    void advanceProgress(long progress) override;
    void prepareCheckpoint() override;
    void fireWindow(int64_t windowEnd) override;
    void clearWindow(int64_t windowEnd) override;
    void close() override;
    TypeSerializer* createWindowSerializer() override;
    Output* getOutput() override;

private:
    // One input row staged for the grouped state write in processBatch.
    struct PendingRow {
        KeyType key;      // partition key (shared BinaryRowData, SINGLETON arity-0 when empty)
        int64_t windowEnd;
        UK sortKey;       // projected sort key row (BinaryRowData, content-addressed)
        RowData* row;     // full input row copy (BinaryRowData)
    };

    // Per (key, window) group loaded once per batch.
    struct WindowGroup {
        KeyType key;
        int64_t windowEnd = 0;
        StateMap* map = nullptr; // HEAP: live map owned by the state table;
                                 // ROCKSDB: entries() copy (borrowed) or owned map.
        bool mapOwned = false;   // ROCKSDB only: true when allocated by us (state was empty)
    };

    void loadGroupMap(WindowGroup& group);
    void appendRowToGroup(WindowGroup& group, PendingRow& pending);
    void flushGroup(WindowGroup& group);
    void releaseGroupMemory(WindowGroup& group, bool deleteRows);

    bool sortKeyLess(UK a, UK b) const;
    int compareSortKeyColumn(UK a, UK b, size_t pos) const;
    UK projectSortKey(RowData* row) const;

    omnistream::VectorBatch* buildOutputBatch(const std::vector<std::pair<RowData*, int64_t>>& collected) const;
    void copyRowColumn(omnistream::VectorBatch* batch, int col, int rowIdx, RowData* row) const;

    // description contract
    std::vector<int32_t> partitionKeyIndices_;
    std::vector<int32_t> sortFieldIndices_;
    std::vector<bool> sortAscendingOrders_;
    std::vector<bool> sortNullsIsLast_;
    std::vector<omniruntime::type::DataTypeId> sortFieldTypeIds_;
    int64_t rankStart_ = 1;
    int64_t rankEnd_ = 1;
    bool outputRankNumber_ = false;
    int32_t windowEndIndex_ = -1;
    std::string shiftTimeZone_ = "UTC";

    std::vector<std::string> inputTypes_;
    std::vector<omniruntime::type::DataTypeId> inputTypeIds_;
    std::vector<std::string> outputTypes_;
    std::vector<omniruntime::type::DataTypeId> outputTypeIds_;

    std::unique_ptr<KeySelector<KeyType>> keySelector_;
    std::unique_ptr<WindowMapState<KeyType, int64_t, UK, UV>> windowState_;
    std::unique_ptr<TimestampedCollector> collector_;

    AbstractKeyedStateBackend<KeyType>* stateBackend_ = nullptr;
    InternalTimerServiceImpl<KeyType, int64_t>* internalTimerService_ = nullptr;
    omnistream::StateType backendType_ = omnistream::StateType::HEAP;
    int64_t currentProgress = INT64_MIN;
    Output* output;
};
