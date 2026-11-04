/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#pragma once
#include <set>
#include <unordered_map>
#include <vector>
#include <cstring>
#include <arm_sve.h>

#include "table/runtime/operators/join/stream/AbstractStreamingJoinOperator.h"
#include "table/data/JoinedRowData.h"
#include "table/data/GenericRowData.h"
#include "table/data/util/RowDataUtil.h"
#include "table/typeutils/RowDataSerializer.h"
#include "streaming/api/operators/Triggerable.h"
#include "table/runtime/operators/InternalTimerServiceImpl.h"
#include "core/typeutils/LongSerializer.h"
#include "table/runtime/operators/TableStreamOperator.h"
#include "table/runtime/operators/window/state/WindowListState.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "table/data/util/VectorBatchUtil.h"
#include "core/api/common/state/ListStateDescriptor.h"
#include "OmniOperatorJIT/core/src/codegen/simple_filter_codegen.h"
#include "OmniOperatorJIT/core/src/vector/unsafe_vector.h"
#include "OmniOperatorJIT/core/src/operator/execution_context.h"
#include "table/runtime/keyselector/KeySelector.h"
#include "core/utils/key_type_traits.h"
#include "table/utils/TimeWindowUtil.h"
#include "runtime/metrics/SimpleCounter.h"
using namespace omnistream;
using FilterFunc = bool (*)(int64_t*, bool*, int32_t*, bool*, int32_t*, int64_t);
namespace {
template <typename TYPE>
TYPE getRowValue(RowData* row, int colIdx);

template <>
inline short getRowValue<short>(RowData* row, int colIdx)
{
    return static_cast<short>(*row->getInt(colIdx));
}

template <>
inline int32_t getRowValue<int32_t>(RowData* row, int colIdx)
{
    return *row->getInt(colIdx);
}

template <>
inline int64_t getRowValue<int64_t>(RowData* row, int colIdx)
{
    return *row->getLong(colIdx);
}

template <>
inline double getRowValue<double>(RowData* row, int colIdx)
{
    int64_t longBits = *row->getLong(colIdx);
    double result;
    std::memcpy(&result, &longBits, sizeof(double));
    return result;
}

template <>
inline bool getRowValue<bool>(RowData* row, int colIdx)
{
    return *row->getInt(colIdx) != 0;
}
template <>
inline omniruntime::type::Decimal128 getRowValue<omniruntime::type::Decimal128>(RowData* row, int colIdx)
{
    return *row->getDecimal128(colIdx);
}

inline void extractRowValueToFilterArray(
    RowData* row,
    int rowColIdx,    // Index inside RowData
    int filterColIdx, // Target index inside the `vals`/`lengths`/`nulls` arrays
    int64_t* vals,
    int32_t* lengths,
    uint8_t* nulls,
    omniruntime::type::DataTypeId typeId)
{
    if (row->isNullAt(rowColIdx)) {
        nulls[filterColIdx] = 1;
        vals[filterColIdx] = 0;
        lengths[filterColIdx] = 0;
        return;
    }

    nulls[filterColIdx] = 0;

    switch (typeId) {
        case omniruntime::type::DataTypeId::OMNI_INT:
        case omniruntime::type::DataTypeId::OMNI_DATE32:
            vals[filterColIdx] = reinterpret_cast<int64_t>(row->getInt(rowColIdx));
            break;
        case omniruntime::type::DataTypeId::OMNI_LONG:
        case omniruntime::type::DataTypeId::OMNI_TIMESTAMP:
        case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
        case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            vals[filterColIdx] = reinterpret_cast<int64_t>(row->getLong(rowColIdx));
            break;
        case omniruntime::type::DataTypeId::OMNI_DOUBLE: {
            vals[filterColIdx] = reinterpret_cast<int64_t>(row->getLong(rowColIdx));
            break;
        }
        case omniruntime::type::DataTypeId::OMNI_BOOLEAN:
            vals[filterColIdx] = reinterpret_cast<int64_t>(row->getInt(rowColIdx));
            break;
        case omniruntime::type::DataTypeId::OMNI_CHAR:
        case omniruntime::type::DataTypeId::OMNI_VARCHAR: {
            // The codegen reads string columns as a raw `i8*` buffer plus its length from the
            // lengths array (SimpleFilterCodeGen::Visit), so the raw data pointer and the size
            // must be filled here.
            std::string_view sv = row->getStringView(rowColIdx);
            vals[filterColIdx] = reinterpret_cast<int64_t>(sv.data());
            lengths[filterColIdx] = static_cast<int32_t>(sv.size());
            break;
        }
        case omniruntime::type::DataTypeId::OMNI_DECIMAL128:
            // The codegen loads a 128-bit decimal from the address stored in `vals`
            // (GetPtrTypeFromInt maps OMNI_DECIMAL128 to i128*).
            vals[filterColIdx] = reinterpret_cast<int64_t>(row->getDecimal128(rowColIdx));
            break;
        default:
            THROW_LOGIC_EXCEPTION(
                "Unsupported type in join filter evaluation: " + std::to_string(static_cast<int>(typeId)));
    }
}

} // end anonymous namespace

template <typename KeyType>
class WindowJoinOperator : public TableStreamOperator<KeyType>,
                           public Triggerable<KeyType, int64_t>,
                           public TwoInputStreamOperator {
public:
    WindowJoinOperator(
        const nlohmann::json& config, Output* output, TypeSerializer* leftSerializer, TypeSerializer* rightSerializer);
    ~WindowJoinOperator() override;
    void open() override;
    void close() override;
    void processBatch1(StreamRecord* input) override;
    void processBatch2(StreamRecord* input) override;

    void processElement1(StreamRecord* element) override {};

    void processElement2(StreamRecord* element) override {};

    void ProcessWatermark1(Watermark* watermark) override
    {
        if (this->combinedWatermark->UpdateWatermark(0, watermark->getTimestamp())) {
            auto newWatermark = Watermark(this->combinedWatermark->GetCombinedWatermark());
            if (this->timeServiceManager != nullptr) {
                this->timeServiceManager->advanceWatermark(&newWatermark);
            }
            this->output->emitWatermark(&newWatermark);
        }
    }

    void ProcessWatermark2(Watermark* watermark) override
    {
        if (this->combinedWatermark->UpdateWatermark(1, watermark->getTimestamp())) {
            auto newWatermark = Watermark(this->combinedWatermark->GetCombinedWatermark());
            if (this->timeServiceManager != nullptr) {
                this->timeServiceManager->advanceWatermark(&newWatermark);
            }
            this->output->emitWatermark(&newWatermark);
        }
    }

    void onEventTime(TimerHeapInternalTimer<KeyType, int64_t>* timer) override;
    void onProcessingTime(TimerHeapInternalTimer<KeyType, int64_t>* timer) override;
    void initializeState(StreamTaskStateInitializerImpl* initializer, TypeSerializer* keySerializer) override;
    virtual void join(std::vector<RowData*>* leftRecords, std::vector<RowData*>* rightRecords) = 0;
    std::string getTypeName() override
    {
        return "WindowJoinOperator";
    }

    InternalTimerServiceImpl<KeyType, int64_t>* getInternalTimerService()
    {
        return internalTimerService;
    };

    std::shared_ptr<omnistream::TaskMetricGroup> GetMectrics() override
    {
        LOG("WindowJoinOperator GetMectrics");
        return this->metrics;
    }

protected:
    TimestampedCollector* collector;
    std::vector<omniruntime::type::DataTypeId> leftTypes;
    std::vector<omniruntime::type::DataTypeId> rightTypes;
    std::vector<omniruntime::type::DataTypeId> outputTypes;
    // Uses int64_t as the template type but not omnistream::ComboId to keep compatibility with existing code.
    WindowListState<KeyType, int64_t, RowData*>* leftWindowState;
    WindowListState<KeyType, int64_t, RowData*>* rightWindowState;
    ::FilterFunc generatedFilter; // Use global scope resolution to avoid ambiguity
    std::vector<int32_t> leftKeyIndex;
    std::vector<int32_t> rightKeyIndex;
    void buildInner(
        std::vector<RowData*>* leftElements,
        std::vector<RowData*>* rightElements,
        omnistream::VectorBatch* outputBatch,
        bool rightMajor = false);
    void buildRightNull(std::vector<RowData*>* leftElements, omnistream::VectorBatch* outputBatch);
    void buildLeftNull(std::vector<RowData*>* rightElements, omnistream::VectorBatch* outputBatch);
    void buildSemiAnti(
        std::vector<RowData*>* elements, omnistream::VectorBatch* outputBatch, bool isSemi, std::set<int>* matchedSet);
    bool filter(RowData* leftElement, RowData* rightElement);
    // Aligns with Flink JoinConditionWithNullFilters: a row whose filtered join key column
    // is null can never match any row on the other side. isLeft selects the left/right key
    // column index list.
    //
    // Row keys (BinaryRowData) preserve the null flag, so NULL keys form their own key group
    // and the planner-declared filterNullKeys is the only thing that decides whether null rows
    // match (all-false means the null group matches, mirroring Flink exactly). Scalar keys
    // (int32_t/int64_t) cannot carry the null flag: KeySelector reads the raw value, so a NULL
    // key collapses into an arbitrary group and can mix with real rows. For scalar keys every
    // actual null key is therefore treated as filtered, so the group is split instead of being
    // fast-pathed into a false cartesian.
    bool hasFilteredNullKey(RowData* row, bool isLeft);
    // Scans a whole group for rows with a filtered null key. Used to decide whether the
    // uniform-key fast paths are safe: only when no row on either side carries a filtered
    // null key do all rows of the group share the same (non-null) join key. For scalar keys
    // the check is data-driven (actual nulls, independent of filterNullKeys) because the null
    // flag cannot be represented in the key and null rows may share a group with real rows.
    bool hasAnyFilteredNull(const std::vector<RowData*>* records, bool isLeft);
    InternalTimerServiceImpl<KeyType, int64_t>* internalTimerService;
    bool isNonEquiCondition;
    std::unique_ptr<KeySelector<KeyType>> keySelectorLeft{};
    std::unique_ptr<KeySelector<KeyType>> keySelectorRight{};

private:
    TypeSerializer* leftSerializer;
    TypeSerializer* rightSerializer;
    int leftWindowEndIndex;
    int rightWindowEndIndex;
    nlohmann::json description;
    std::set<int> colRefsForNonEquiCondition;
    int totalNumOfCols;
    std::shared_ptr<Counter> leftNumLateRecordsDropped;
    std::shared_ptr<Counter> rightNumLateRecordsDropped;
    struct BatchCleanupInfo {
        int32_t keyGroup;
        uint32_t sequenceNumber;
        int64_t maxTimestamp;
    };
    // Vector, rowId, colId, valAddressPtr, isNullPtr
    std::vector<void (*)(omniruntime::vec::BaseVector*, int32_t, int32_t, int64_t*, bool*)> filterFuncPtrs;
    std::vector<bool> filterNullKeys;
    std::vector<BatchCleanupInfo> leftBatchCleanupInfos;
    std::vector<BatchCleanupInfo> rightBatchCleanupInfos;
    std::string shiftTimeZone = "UTC";
    omnistream::StateType stateType_ = omnistream::StateType::HEAP;
    int32_t maxParallelism_ = 0;
    std::unordered_map<int32_t, omnistream::VectorBatch*> reuseVectorBatchByKeyGroup_{};
    std::unordered_map<int32_t, std::vector<int32_t>> reuseOldRowIdsByKeyGroup_{};
    std::vector<KeyType> reuseKeys_{};
    std::vector<ComboId> reuseComboIds_{};
    std::vector<int64_t> reuseWindowEndTimes_{};

    template <typename TYPE>
    void insertLeft(
        int colIdx,
        std::vector<RowData*>* leftElements,
        std::vector<RowData*>* rightElements,
        omnistream::VectorBatch* outputBatch,
        bool isInner,
        bool rightMajor = false);
    template <typename TYPE>
    void insertRight(
        int colIdx,
        std::vector<RowData*>* leftElements,
        std::vector<RowData*>* rightElements,
        omnistream::VectorBatch* outputBatch,
        bool isInner,
        bool rightMajor = false);
    void insertLeftVarchar(
        int colIdx,
        std::vector<RowData*>* leftElements,
        std::vector<RowData*>* rightElements,
        omnistream::VectorBatch* outputBatch,
        bool isInner,
        bool rightMajor = false);
    void insertRightVarchar(
        int colIdx,
        std::vector<RowData*>* leftElements,
        std::vector<RowData*>* rightElements,
        omnistream::VectorBatch* outputBatch,
        bool isInner,
        bool rightMajor = false);

    void processBatch(
        omnistream::VectorBatch* batch,
        int windowEndIndex,
        WindowListState<KeyType, int64_t, RowData*>* recordState,
        bool isLeftSide);
    ::FilterFunc generateJoinCondition();
    void getAllColRefs(nlohmann::json& config);
    void BuildInnerLeft(
        std::vector<RowData*>* leftElements,
        std::vector<RowData*>* rightElements,
        omnistream::VectorBatch* outputBatch,
        bool rightMajor = false);
    void BuildInnerRight(
        std::vector<RowData*>* leftElements,
        std::vector<RowData*>* rightElements,
        omnistream::VectorBatch* outputBatch,
        bool rightMajor = false);
};

template <typename KeyType>
WindowJoinOperator<KeyType>::WindowJoinOperator(
    const nlohmann::json& config, Output* output, TypeSerializer* leftSerializer, TypeSerializer* rightSerializer)
    : TableStreamOperator<KeyType>(new TimestampedCollector(output)),
      isNonEquiCondition(config.contains("nonEquiCondition") && !config["nonEquiCondition"].is_null()),
      leftSerializer(leftSerializer),
      rightSerializer(rightSerializer),
      leftWindowEndIndex(config["leftWindowEndIndex"]),
      rightWindowEndIndex(config["rightWindowEndIndex"]),
      description(config),
      colRefsForNonEquiCondition(),
      totalNumOfCols(),
      filterFuncPtrs()
{
    auto leftTypeStr = config["leftInputTypes"].get<std::vector<std::string>>();
    auto rightTypeStr = config["rightInputTypes"].get<std::vector<std::string>>();
    rightKeyIndex = description["rightJoinKey"].get<std::vector<int32_t>>();
    leftKeyIndex = description["leftJoinKey"].get<std::vector<int32_t>>();
    if (description.contains("filterNulls")) {
        filterNullKeys = description["filterNulls"].get<std::vector<bool>>();
    } else {
        // Default to filtering null keys on all join keys, matching SQL '=' semantics.
        filterNullKeys.assign(leftKeyIndex.size(), true);
    }
    for (const auto& i : leftTypeStr) {
        leftTypes.push_back(LogicalType::flinkTypeToOmniTypeId(i));
    }

    for (const auto& i : rightTypeStr) {
        rightTypes.push_back(LogicalType::flinkTypeToOmniTypeId(i));
    }
    if (config.contains("shiftTimeZone")) {
        shiftTimeZone = config["shiftTimeZone"].get<std::string>();
    }
    outputTypes.insert(outputTypes.end(), this->leftTypes.begin(), this->leftTypes.end());
    outputTypes.insert(outputTypes.end(), this->rightTypes.begin(), this->rightTypes.end());
    if (leftSerializer == nullptr) {
        INFO_RELEASE("leftSerializer is null");
        this->leftSerializer = new RowDataSerializer(new omnistream::RowType(true, leftTypeStr));
    }
    if (rightSerializer == nullptr) {
        INFO_RELEASE("rightSerializer is null");
        this->rightSerializer = new RowDataSerializer(new omnistream::RowType(true, rightTypeStr));
    }
}

template <typename KeyType>
WindowJoinOperator<KeyType>::~WindowJoinOperator()
{
    delete collector;
    delete leftSerializer;
    delete rightSerializer;
}

template <typename KeyType>
void WindowJoinOperator<KeyType>::open()
{
    totalNumOfCols = leftTypes.size() + rightTypes.size();

    collector = new TimestampedCollector(this->output);
    collector->eraseTimestamp();
    internalTimerService =
        AbstractStreamOperator<KeyType>::getInternalTimerService("window-timers", new LongSerializer(), this);
    std::string leftName = "left-records";
    std::string rightName = "right-records";

    auto leftDescriptor = new ListStateDescriptor<RowData*>(leftName, leftSerializer);
    auto rightDescriptor = new ListStateDescriptor<RowData*>(rightName, rightSerializer);

    using S = InternalListState<KeyType, int64_t, RowData*>;
    auto keyedStateBackend = this->stateHandler->getKeyedStateBackend();
    stateType_ = keyedStateBackend->getStateType();
    omnistream::checkStateType(stateType_, "WindowJoinOperator");
    leftWindowState = new WindowListState<KeyType, int64_t, RowData*>(
        keyedStateBackend->template getOrCreateKeyedState<int64_t, S, std::vector<RowData*>*>(
            new LongSerializer(), leftDescriptor));
    rightWindowState = new WindowListState<KeyType, int64_t, RowData*>(
        keyedStateBackend->template getOrCreateKeyedState<int64_t, S, std::vector<RowData*>*>(
            new LongSerializer(), rightDescriptor));

    std::vector<int> leftKeyTypes;
    std::vector<int> rightKeyTypes;
    for (auto kIndex : this->leftKeyIndex) {
        leftKeyTypes.push_back(this->leftTypes[kIndex]);
    }
    for (auto kIndex : this->rightKeyIndex) {
        rightKeyTypes.push_back(this->rightTypes[kIndex]);
    }
    // make sure the key types are the same
    if (leftKeyTypes != rightKeyTypes) {
        throw std::runtime_error("Left key types do not match right key types");
    }
    this->keySelectorLeft = std::make_unique<KeySelector<KeyType>>(leftKeyTypes, this->leftKeyIndex);
    this->keySelectorRight = std::make_unique<KeySelector<KeyType>>(rightKeyTypes, this->rightKeyIndex);
    maxParallelism_ =
        static_cast<StreamingRuntimeContext<KeyType>*>(this->getRuntimeContext())->getMaxNumberOfSubtasks();

    generatedFilter = generateJoinCondition();
    getAllColRefs(description["nonEquiCondition"]);

    for (int i = 0; i < totalNumOfCols; i++) {
        if (colRefsForNonEquiCondition.find(i) == colRefsForNonEquiCondition.end()) {
            filterFuncPtrs.push_back(nullptr);
        } else {
            bool leftSideState = static_cast<size_t>(i) < leftTypes.size();
            switch (leftSideState ? leftTypes[i] : rightTypes[i - leftTypes.size()]) {
                case omniruntime::type::DataTypeId::OMNI_INT:
                case omniruntime::type::DataTypeId::OMNI_DATE32:
                    filterFuncPtrs.push_back(getValueAddress<int32_t>);
                    break;
                case omniruntime::type::DataTypeId::OMNI_LONG:
                    filterFuncPtrs.push_back(getValueAddress<int64_t>);
                    break;
                case omniruntime::type::DataTypeId::OMNI_DOUBLE:
                    filterFuncPtrs.push_back(getValueAddress<double>);
                    break;
                case omniruntime::type::DataTypeId::OMNI_BOOLEAN:
                    filterFuncPtrs.push_back(getValueAddress<bool>);
                    break;
                default:
                    // filterFuncPtrs is currently unused by the row-based filter() path; keep
                    // non-fatal for types not handled here (e.g. CHAR/VARCHAR/DECIMAL128).
                    filterFuncPtrs.push_back(nullptr);
                    break;
            }
        }
    }

    // Metrics (aligned with Flink WindowJoinOperator: late records dropped counters; the rate
    // meters are not implemented natively yet).
    // This base registration applies to all join types (Inner/Left/Right/Full/Semi/Anti),
    // so LeftOuter and RightOuter expose the identical metric set.
    if (this->metrics != nullptr) {
        leftNumLateRecordsDropped = std::make_shared<omnistream::SimpleCounter>();
        rightNumLateRecordsDropped = std::make_shared<omnistream::SimpleCounter>();
        this->metrics->AddInternalOperatorIOMetric(
            this->getTypeName(), "leftNumLateRecordsDropped", leftNumLateRecordsDropped);
        this->metrics->AddInternalOperatorIOMetric(
            this->getTypeName(), "rightNumLateRecordsDropped", rightNumLateRecordsDropped);
    }
}

template <typename KeyType>
void WindowJoinOperator<KeyType>::close()
{
    AbstractStreamOperator<KeyType>::close();
    collector = nullptr;
}

template <typename KeyType>
void WindowJoinOperator<KeyType>::processBatch1(StreamRecord* input)
{
    auto record = std::unique_ptr<StreamRecord>(input);
    auto batch = reinterpret_cast<omnistream::VectorBatch*>(record->getValue());
    processBatch(batch, leftWindowEndIndex, leftWindowState, true);
}

template <typename KeyType>
void WindowJoinOperator<KeyType>::processBatch2(StreamRecord* input)
{
    auto record = std::unique_ptr<StreamRecord>(input);
    auto batch = reinterpret_cast<omnistream::VectorBatch*>(record->getValue());
    processBatch(batch, rightWindowEndIndex, rightWindowState, false);
}

template <typename KeyType>
inline void WindowJoinOperator<KeyType>::initializeState(
    StreamTaskStateInitializerImpl* initializer, TypeSerializer* keySerializer)
{
    // First do the shared initialization step
    INFO_RELEASE(
        "WindowJoinOperator initializeState with initializer, operatorID: "
        << TwoInputStreamOperator::GetOperatorID().toString());
    AbstractStreamOperator<KeyType>::SetOperatorID(TwoInputStreamOperator::GetOperatorID().toString());
    AbstractStreamOperator<KeyType>::initializeState(initializer, keySerializer);
}

template <typename KeyType>
void WindowJoinOperator<KeyType>::onEventTime(TimerHeapInternalTimer<KeyType, int64_t>* timer)
{
    int64_t window = timer->getNamespace();
    this->setCurrentKey(timer->getKey());

    // 内存状态后端情况下，不需要手动做删除
    std::vector<RowData*>* leftRecords = leftWindowState->get(window);
    std::vector<RowData*>* rightRecords = rightWindowState->get(window);

    if (leftRecords != nullptr) {
        LOG_PRINTF("onEventTime from left %d", leftRecords->size());
    } else {
        LOG_PRINTF("onEventTime from left 0");
    }
    if (rightRecords != nullptr) {
        LOG_PRINTF("onEventTime from right %d", rightRecords->size());
    } else {
        LOG_PRINTF("onEventTime from right 0");
    }
    join(leftRecords, rightRecords);
    if (leftRecords != nullptr) {
        for (RowData* row : *leftRecords) {
            delete row;
        }
        leftWindowState->clear(window);
        if (stateType_ != omnistream::StateType::HEAP) {
            delete leftRecords;
        }
    }
    if (rightRecords != nullptr) {
        for (RowData* row : *rightRecords) {
            delete row;
        }
        rightWindowState->clear(window);
        if (stateType_ != omnistream::StateType::HEAP) {
            delete rightRecords;
        }
    }

    std::unordered_map<int32_t, std::vector<uint32_t>> leftSequenceNumbersToDelete;
    for (auto& info : leftBatchCleanupInfos) {
        if (window > info.maxTimestamp && info.maxTimestamp != INT64_MIN) {
            leftSequenceNumbersToDelete[info.keyGroup].push_back(info.sequenceNumber);
            info.maxTimestamp = INT64_MAX;
        }
    }
    for (auto& [keyGroup, sequenceNumbers] : leftSequenceNumbersToDelete) {
        leftWindowState->clearVectorBatches(keyGroup, sequenceNumbers);
    }
    std::unordered_map<int32_t, std::vector<uint32_t>> rightSequenceNumbersToDelete;
    for (auto& info : rightBatchCleanupInfos) {
        if (window > info.maxTimestamp && info.maxTimestamp != INT64_MIN) {
            rightSequenceNumbersToDelete[info.keyGroup].push_back(info.sequenceNumber);
            info.maxTimestamp = INT64_MAX;
        }
    }
    for (auto& [keyGroup, sequenceNumbers] : rightSequenceNumbersToDelete) {
        rightWindowState->clearVectorBatches(keyGroup, sequenceNumbers);
    }
}

template <typename KeyType>
void WindowJoinOperator<KeyType>::onProcessingTime(TimerHeapInternalTimer<KeyType, int64_t>* timer)
{
    THROW_LOGIC_EXCEPTION("Window Join only support event-time now");
}

/*
BuildInner() Example:
key is first column

left:
1, 2
1, 3
1, 4

right:
1, 0
1, 5

result:
(1, 2)(1, 0)
(1, 2)(1, 5)
(1, 3)(1, 0)
(1, 3)(1, 5)
(1, 4)(1, 0)
(1, 4)(1, 5)
*/
template <typename KeyType>
void WindowJoinOperator<KeyType>::BuildInnerLeft(
    std::vector<RowData*>* leftElements, std::vector<RowData*>* rightElements, omnistream::VectorBatch* outputBatch,
    bool rightMajor)
{
    int colIdx = 0;
    // Left side
    for (auto dataType : leftTypes) {
        switch (dataType) {
            case omniruntime::type::DataTypeId::OMNI_SHORT:
                insertLeft<int16_t>(colIdx, leftElements, rightElements, outputBatch, true, rightMajor);
                break;
            case omniruntime::type::DataTypeId::OMNI_INT:
            case omniruntime::type::DataTypeId::OMNI_DATE32:
                insertLeft<int32_t>(colIdx, leftElements, rightElements, outputBatch, true, rightMajor);
                break;
            case omniruntime::type::DataTypeId::OMNI_LONG:
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP:
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                insertLeft<int64_t>(colIdx, leftElements, rightElements, outputBatch, true, rightMajor);
                break;
            case omniruntime::type::DataTypeId::OMNI_DOUBLE:
                insertLeft<double>(colIdx, leftElements, rightElements, outputBatch, true, rightMajor);
                break;
            case omniruntime::type::DataTypeId::OMNI_BOOLEAN:
                insertLeft<bool>(colIdx, leftElements, rightElements, outputBatch, true, rightMajor);
                break;
            case omniruntime::type::DataTypeId::OMNI_DECIMAL128:
                insertLeft<omniruntime::type::Decimal128>(colIdx, leftElements, rightElements, outputBatch, true, rightMajor);
                break;
            case omniruntime::type::DataTypeId::OMNI_CHAR:
            case omniruntime::type::DataTypeId::OMNI_VARCHAR:
                insertLeftVarchar(colIdx, leftElements, rightElements, outputBatch, true, rightMajor);
                break;
            default: THROW_LOGIC_EXCEPTION("Type not recognized"); break;
        }
        colIdx++;
    }
}

template <typename KeyType>
void WindowJoinOperator<KeyType>::BuildInnerRight(
    std::vector<RowData*>* leftElements, std::vector<RowData*>* rightElements, omnistream::VectorBatch* outputBatch,
    bool rightMajor)
{
    // Right side
    int colIdx = leftTypes.size();
    for (auto dataType : rightTypes) {
        switch (dataType) {
            case omniruntime::type::DataTypeId::OMNI_SHORT:
                insertRight<int16_t>(colIdx, leftElements, rightElements, outputBatch, true, rightMajor);
                break;
            case omniruntime::type::DataTypeId::OMNI_INT:
            case omniruntime::type::DataTypeId::OMNI_DATE32:
                insertRight<int32_t>(colIdx, leftElements, rightElements, outputBatch, true, rightMajor);
                break;
            case omniruntime::type::DataTypeId::OMNI_LONG:
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP:
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                insertRight<int64_t>(colIdx, leftElements, rightElements, outputBatch, true, rightMajor);
                break;
            case omniruntime::type::DataTypeId::OMNI_DOUBLE:
                insertRight<double>(colIdx, leftElements, rightElements, outputBatch, true, rightMajor);
                break;
            case omniruntime::type::DataTypeId::OMNI_BOOLEAN:
                insertRight<bool>(colIdx, leftElements, rightElements, outputBatch, true, rightMajor);
                break;
            case omniruntime::type::DataTypeId::OMNI_DECIMAL128:
                insertRight<omniruntime::type::Decimal128>(colIdx, leftElements, rightElements, outputBatch, true, rightMajor);
                break;
            case omniruntime::type::DataTypeId::OMNI_CHAR:
            case omniruntime::type::DataTypeId::OMNI_VARCHAR:
                insertRightVarchar(colIdx, leftElements, rightElements, outputBatch, true, rightMajor);
                break;
            default: THROW_LOGIC_EXCEPTION("Type not recognized"); break;
        }
        colIdx++;
    }
}

template <typename KeyType>
void WindowJoinOperator<KeyType>::buildInner(
    std::vector<RowData*>* leftElements, std::vector<RowData*>* rightElements, omnistream::VectorBatch* outputBatch,
    bool rightMajor)
{
    BuildInnerLeft(leftElements, rightElements, outputBatch, rightMajor);
    BuildInnerRight(leftElements, rightElements, outputBatch, rightMajor);
}

template <typename KeyType>
void WindowJoinOperator<KeyType>::buildRightNull(
    std::vector<RowData*>* leftElements, omnistream::VectorBatch* outputBatch)
{
    int colIdx = 0;
    for (auto dataType : leftTypes) {
        switch (dataType) {
            case omniruntime::type::DataTypeId::OMNI_SHORT:
                insertLeft<int16_t>(colIdx, leftElements, nullptr, outputBatch, false);
                break;
            case omniruntime::type::DataTypeId::OMNI_INT:
            case omniruntime::type::DataTypeId::OMNI_DATE32:
                insertLeft<int32_t>(colIdx, leftElements, nullptr, outputBatch, false);
                break;
            case omniruntime::type::DataTypeId::OMNI_LONG:
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP:
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                insertLeft<int64_t>(colIdx, leftElements, nullptr, outputBatch, false);
                break;
            case omniruntime::type::DataTypeId::OMNI_DOUBLE:
                insertLeft<double>(colIdx, leftElements, nullptr, outputBatch, false);
                break;
            case omniruntime::type::DataTypeId::OMNI_BOOLEAN:
                insertLeft<bool>(colIdx, leftElements, nullptr, outputBatch, false);
                break;
            case omniruntime::type::DataTypeId::OMNI_DECIMAL128:
                insertLeft<omniruntime::type::Decimal128>(colIdx, leftElements, nullptr, outputBatch, false);
                break;
            case omniruntime::type::DataTypeId::OMNI_CHAR:
            case omniruntime::type::DataTypeId::OMNI_VARCHAR:
                insertLeftVarchar(colIdx, leftElements, nullptr, outputBatch, false);
                break;
            default: THROW_LOGIC_EXCEPTION("Type not recognized"); break;
        }
        colIdx++;
    }

    for (int i = leftTypes.size(); i < rightTypes.size() + leftTypes.size(); i++) {
        for (int j = 0; j < outputBatch->Get(i)->GetSize(); j++) {
            outputBatch->Get(i)->SetNull(j);
        }
    }
}

template <typename KeyType>
void WindowJoinOperator<KeyType>::buildLeftNull(
    std::vector<RowData*>* rightElements, omnistream::VectorBatch* outputBatch)
{
    int colIdx = 0 + leftTypes.size();
    for (auto dataType : rightTypes) {
        switch (dataType) {
            case omniruntime::type::DataTypeId::OMNI_SHORT:
                insertRight<int16_t>(colIdx, nullptr, rightElements, outputBatch, false);
                break;
            case omniruntime::type::DataTypeId::OMNI_INT:
            case omniruntime::type::DataTypeId::OMNI_DATE32:
                insertRight<int32_t>(colIdx, nullptr, rightElements, outputBatch, false);
                break;
            case omniruntime::type::DataTypeId::OMNI_LONG:
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP:
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                insertRight<int64_t>(colIdx, nullptr, rightElements, outputBatch, false);
                break;
            case omniruntime::type::DataTypeId::OMNI_DOUBLE:
                insertRight<double>(colIdx, nullptr, rightElements, outputBatch, false);
                break;
            case omniruntime::type::DataTypeId::OMNI_BOOLEAN:
                insertRight<bool>(colIdx, nullptr, rightElements, outputBatch, false);
                break;
            case omniruntime::type::DataTypeId::OMNI_DECIMAL128:
                insertRight<omniruntime::type::Decimal128>(colIdx, nullptr, rightElements, outputBatch, false);
                break;
            case omniruntime::type::DataTypeId::OMNI_CHAR:
            case omniruntime::type::DataTypeId::OMNI_VARCHAR:
                insertRightVarchar(colIdx, nullptr, rightElements, outputBatch, false);
                break;
            default: THROW_LOGIC_EXCEPTION("Type not recognized"); break;
        }
        colIdx++;
    }

    for (int i = 0; i < leftTypes.size(); i++) {
        for (int j = 0; j < outputBatch->Get(i)->GetSize(); j++) {
            outputBatch->Get(i)->SetNull(j);
        }
    }
}

template <typename KeyType>
inline void WindowJoinOperator<KeyType>::buildSemiAnti(
    std::vector<RowData*>* elements, omnistream::VectorBatch* outputBatch, bool isSemi, std::set<int>* matchedSet)
{
}

template <typename KeyType>
void WindowJoinOperator<KeyType>::processBatch(
    omnistream::VectorBatch* batch,
    int windowEndIndex,
    WindowListState<KeyType, int64_t, RowData*>* recordState,
    bool isLeftSide)
{
    auto batchGuard = std::unique_ptr<omnistream::VectorBatch>(batch);
    KeySelector<KeyType>* keySelector = nullptr;
    keySelector = isLeftSide ? this->keySelectorLeft.get() : this->keySelectorRight.get();

    auto rowCount = batchGuard->GetRowCount();

    for (int i = 0; i < rowCount; i++) {
        // processElement();
        int64_t windowEndTime =
            reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(batchGuard->Get(windowEndIndex))->GetValue(i);
        if (TimeWindowUtil::isWindowFired(windowEndTime, internalTimerService->currentWatermark(), shiftTimeZone)) {
            // Aligns with Flink: late elements are dropped (and counted) per side.
            if (isLeftSide) {
                if (leftNumLateRecordsDropped != nullptr) {
                    leftNumLateRecordsDropped->Inc();
                }
            } else {
                if (rightNumLateRecordsDropped != nullptr) {
                    rightNumLateRecordsDropped->Inc();
                }
            }
            continue;
        }
        // reuseWindowEndTimes_[i] = windowEndTime;
        auto key = keySelector->getKey(batchGuard.get(), i);

        this->setCurrentKey(key);
        auto inputRow = std::unique_ptr<RowData>(batchGuard->extractRowData(i));
        if (!RowDataUtil::isAccumulateMsg(inputRow->getRowKind())) {
            // Aligns with Flink: window join cannot handle retraction (DELETE/UPDATE_BEFORE) input.
            THROW_LOGIC_EXCEPTION("Window join could not handle retraction input stream");
        }
        recordState->add(windowEndTime, inputRow.get());
        if (stateType_ == omnistream::StateType::HEAP) {
            inputRow.release();
        }
        internalTimerService->registerEventTimeTimer(
            windowEndTime, TimeWindowUtil::toEpochMillsForTimer(windowEndTime - 1, shiftTimeZone));
    }
}

template <typename KeyType>
template <typename TYPE>
inline void WindowJoinOperator<KeyType>::insertLeft(
    int colIdx,
    std::vector<RowData*>* leftElements,
    std::vector<RowData*>* rightElements,
    omnistream::VectorBatch* outputBatch,
    bool isInner,
    bool rightMajor)
{
    auto num = leftElements->size();
    auto col = reinterpret_cast<omniruntime::vec::Vector<TYPE>*>(outputBatch->Get(colIdx));
    auto writeRow = [&](RowData* row, int rowIdx) {
        if (row->isNullAt(colIdx)) {
            col->SetNull(rowIdx);
        } else {
            col->SetValue(rowIdx, getRowValue<TYPE>(row, colIdx));
        }
    };
    if (isNonEquiCondition || !isInner) {
        int rowIdx = 0;
        for (int j = 0; j < num; j++) {
            writeRow(leftElements->at(j), rowIdx);
            rowIdx++;
        }
    } else if (rightMajor) {
        // right-major cartesian: row = i * leftSize + j (i = right index, j = left index)
        int rowIdx = 0;
        for (size_t i = 0; i < rightElements->size(); i++) {
            for (int j = 0; j < num; j++) {
                writeRow(leftElements->at(j), rowIdx);
                rowIdx++;
            }
        }
    } else {
        int rowIdx = 0;
        for (int j = 0; j < num; j++) {
            for (size_t i = 0; i < rightElements->size(); i++) {
                writeRow(leftElements->at(j), i + rowIdx);
            }
            rowIdx += rightElements->size();
        }
    }
}

template <typename KeyType>
void WindowJoinOperator<KeyType>::insertLeftVarchar(
    int colIdx,
    std::vector<RowData*>* leftElements,
    std::vector<RowData*>* rightElements,
    omnistream::VectorBatch* outputBatch,
    bool isInner,
    bool rightMajor)
{
    using varcharVecType = omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>;
    auto col = reinterpret_cast<varcharVecType*>(outputBatch->Get(colIdx));
    auto writeRow = [&](RowData* row, int rowIdx) {
        if (row->isNullAt(colIdx)) {
            col->SetNull(rowIdx);
        } else {
            col->SetValue(rowIdx, row->getStringView(colIdx));
        }
    };
    if (isNonEquiCondition || !isInner) {
        int rowIdx = 0;
        for (auto element : *leftElements) {
            writeRow(element, rowIdx);
            rowIdx++;
        }
    } else if (rightMajor) {
        // right-major cartesian: row = i * leftSize + j (i = right index, j = left index)
        int rowIdx = 0;
        auto num = leftElements->size();
        for (size_t i = 0; i < rightElements->size(); i++) {
            for (int j = 0; j < num; j++) {
                writeRow(leftElements->at(j), rowIdx);
                rowIdx++;
            }
        }
    } else {
        int rowIdx = 0;
        auto num = leftElements->size();
        auto keyGroups = std::vector<int32_t>(num);
        auto sequenceNumbers = std::vector<uint32_t>(num);
        auto rowIds = std::vector<int32_t>(num);

        for (int j = 0; j < num; j++) {
            for (size_t i = 0; i < rightElements->size(); i++) {
                writeRow(leftElements->at(j), i + rowIdx);
            }
            rowIdx += rightElements->size();
        }
    }
}

template <typename KeyType>
template <typename TYPE>
inline void WindowJoinOperator<KeyType>::insertRight(
    int colIdx,
    std::vector<RowData*>* leftElements,
    std::vector<RowData*>* rightElements,
    omnistream::VectorBatch* outputBatch,
    bool isInner,
    bool rightMajor)
{
    auto num = rightElements->size();
    auto col = reinterpret_cast<omniruntime::vec::Vector<TYPE>*>(outputBatch->Get(colIdx));
    auto writeRow = [&](RowData* row, int rowIdx) {
        int srcCol = colIdx - leftTypes.size();
        if (row->isNullAt(srcCol)) {
            col->SetNull(rowIdx);
        } else {
            col->SetValue(rowIdx, getRowValue<TYPE>(row, srcCol));
        }
    };
    if (isNonEquiCondition || !isInner) {
        int rowIdx = 0;
        for (int i = 0; i < num; i++) {
            writeRow(rightElements->at(i), rowIdx);
            rowIdx++;
        }
    } else if (rightMajor) {
        // right-major cartesian: row = i * leftSize + j (i = right index, j = left index)
        auto leftSize = leftElements->size();
        for (size_t i = 0; i < rightElements->size(); i++) {
            for (size_t j = 0; j < leftSize; j++) {
                writeRow(rightElements->at(i), i * leftSize + j);
            }
        }
    } else {
        for (size_t i = 0; i < rightElements->size(); i++) {
            for (size_t j = 0; j < leftElements->size(); j++) {
                writeRow(rightElements->at(i), j * rightElements->size() + i);
            }
        }
    }
}

template <typename KeyType>
void WindowJoinOperator<KeyType>::insertRightVarchar(
    int colIdx,
    std::vector<RowData*>* leftElements,
    std::vector<RowData*>* rightElements,
    omnistream::VectorBatch* outputBatch,
    bool isInner,
    bool rightMajor)
{
    using varcharVecType = omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>;
    auto col = reinterpret_cast<varcharVecType*>(outputBatch->Get(colIdx));
    auto writeRow = [&](RowData* row, int rowIdx) {
        int srcCol = colIdx - leftTypes.size();
        if (row->isNullAt(srcCol)) {
            col->SetNull(rowIdx);
        } else {
            col->SetValue(rowIdx, row->getStringView(srcCol));
        }
    };
    if (isNonEquiCondition || !isInner) {
        int rowIdx = 0;
        for (auto element : *rightElements) {
            writeRow(element, rowIdx);
            rowIdx++;
        }
    } else if (rightMajor) {
        // right-major cartesian: row = i * leftSize + j (i = right index, j = left index)
        int rowIdx = 0;
        auto leftSize = leftElements->size();
        for (size_t i = 0; i < rightElements->size(); i++) {
            for (size_t j = 0; j < leftSize; j++) {
                writeRow(rightElements->at(i), rowIdx);
                rowIdx++;
            }
        }
    } else {
        // LargeStringContainer::SetValue only supports sequential ascending writes, so the
        // right-side VARCHAR columns must be filled in row-major order (same layout as
        // insertLeft/insertLeftVarchar: row = j * rightElements->size() + i).
        int rowIdx = 0;
        for (size_t j = 0; j < leftElements->size(); j++) {
            for (size_t i = 0; i < rightElements->size(); i++) {
                writeRow(rightElements->at(i), rowIdx);
                rowIdx++;
            }
        }
    }
}

template <typename KeyType>
::FilterFunc WindowJoinOperator<KeyType>::generateJoinCondition()
{
    if (isNonEquiCondition) {
        auto filter = description["nonEquiCondition"];
        Expr* jExpr = JSONParser::ParseJSON(filter);
        SimpleFilterCodeGen* filterCodegen = new SimpleFilterCodeGen("nonEquiCondition", *jExpr, nullptr);
        int64_t fAddr = filterCodegen->GetFunction();
        void* refFunc = &fAddr;
        return *static_cast<::FilterFunc*>(refFunc);
    }
    return nullptr;
}

template <typename KeyType>
void WindowJoinOperator<KeyType>::getAllColRefs(nlohmann::json& config)
{
    if (config["exprType"] == "FIELD_REFERENCE") {
        colRefsForNonEquiCondition.emplace(config["colVal"]);
    }

    if (config.contains("right")) {
        getAllColRefs(config["right"]);
    }

    if (config.contains("left")) {
        getAllColRefs(config["left"]);
    }
}

template <typename KeyType>
bool WindowJoinOperator<KeyType>::filter(RowData* leftRow, RowData* rightRow)
{
    if (!isNonEquiCondition) {
        return true;
    }

    std::vector<int64_t> vals(totalNumOfCols, 0);
    std::vector<int32_t> lengths(totalNumOfCols, 0);
    std::vector<uint8_t> nulls(totalNumOfCols, 0);
    bool resultBool = false;

    const int leftColsCount = static_cast<int>(leftTypes.size());

    // Populate vals, lengths and nulls directly from RowData
    for (int col : colRefsForNonEquiCondition) {
        if (col < leftColsCount) {
            extractRowValueToFilterArray(leftRow, col, col, vals.data(), lengths.data(), nulls.data(), leftTypes[col]);
        } else {
            int rightColIdx = col - leftColsCount;
            extractRowValueToFilterArray(
                rightRow, rightColIdx, col, vals.data(), lengths.data(), nulls.data(), rightTypes[rightColIdx]);
        }
    }

    omniruntime::op::ExecutionContext context;
    bool result = generatedFilter(
        vals.data(),
        reinterpret_cast<bool*>(nulls.data()),
        lengths.data(),
        &resultBool,
        nullptr,
        reinterpret_cast<int64_t>(&context));

    return result;
}

template <typename KeyType>
bool WindowJoinOperator<KeyType>::hasFilteredNullKey(RowData* row, bool isLeft)
{
    constexpr bool keyCarriesNull =
        KeyTypeTraits<KeyType>::isRowKey || KeyTypeTraits<KeyType>::isSharedRowKey;
    const auto& keyIndexes = isLeft ? leftKeyIndex : rightKeyIndex;
    for (size_t i = 0; i < keyIndexes.size(); i++) {
        if (!row->isNullAt(keyIndexes[i])) {
            continue;
        }
        if constexpr (keyCarriesNull) {
            // Row keys preserve NULL: the NULL row lives in its own key group, and whether it
            // matches is decided solely by the planner-declared filterNullKeys (all-false means
            // the null group matches, mirroring Flink's JoinConditionWithNullFilters).
            if (i < filterNullKeys.size() && filterNullKeys[i]) {
                return true;
            }
        } else {
            // Scalar keys cannot carry the null flag: the raw value is read verbatim, so a NULL
            // row collapses into an arbitrary key group and may mix with real rows. Any actual
            // null key must go through the split path to avoid a false cartesian.
            return true;
        }
    }
    return false;
}

template <typename KeyType>
bool WindowJoinOperator<KeyType>::hasAnyFilteredNull(const std::vector<RowData*>* records, bool isLeft)
{
    if (records == nullptr || records->empty()) {
        return false;
    }
    constexpr bool keyCarriesNull =
        KeyTypeTraits<KeyType>::isRowKey || KeyTypeTraits<KeyType>::isSharedRowKey;
    if constexpr (keyCarriesNull) {
        if (filterNullKeys.empty()) {
            return false;
        }
    }
    for (RowData* row : *records) {
        if (hasFilteredNullKey(row, isLeft)) {
            return true;
        }
    }
    return false;
}
