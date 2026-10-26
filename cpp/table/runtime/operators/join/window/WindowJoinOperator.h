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
#include "table/utils/TimeWindowUtil.h"
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
// todo:Decimal128 type casting?
template <>
inline omniruntime::type::Decimal128 getRowValue<omniruntime::type::Decimal128>(RowData* row, int colIdx)
{
    THROW_LOGIC_EXCEPTION("Extracting Decimal128 directly from RowData is not supported");
    // Dummy return to satisfy the compiler
    return omniruntime::type::Decimal128();
}

inline void extractRowValueToFilterArray(
    RowData* row,
    int rowColIdx,    // Index inside RowData
    int filterColIdx, // Target index inside the `vals`/`nulls` array
    int64_t* vals,
    uint8_t* nulls,
    omniruntime::type::DataTypeId typeId)
{
    if (row->isNullAt(rowColIdx)) {
        nulls[filterColIdx] = 1;
        vals[filterColIdx] = 0;
        return;
    }

    nulls[filterColIdx] = 0;

    switch (typeId) {
        case omniruntime::type::DataTypeId::OMNI_INT:
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
        case omniruntime::type::DataTypeId::OMNI_VARCHAR:
            vals[filterColIdx] = reinterpret_cast<int64_t>(row->getString(rowColIdx));
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
        omnistream::VectorBatch* outputBatch);
    void buildRightNull(std::vector<RowData*>* leftElements, omnistream::VectorBatch* outputBatch);
    void buildLeftNull(std::vector<RowData*>* rightElements, omnistream::VectorBatch* outputBatch);
    void buildSemiAnti(
        std::vector<RowData*>* elements, omnistream::VectorBatch* outputBatch, bool isSemi, std::set<int>* matchedSet);
    bool filter(RowData* leftElement, RowData* rightElement);
    // Aligns with Flink JoinConditionWithNullFilters: if any filtered join key column
    // of the key group is null, no pair in this group can match.
    bool keyHasFilteredNull(const std::vector<RowData*>* leftRecords);
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
        bool isInner);
    template <typename TYPE>
    void insertRight(
        int colIdx,
        std::vector<RowData*>* leftElements,
        std::vector<RowData*>* rightElements,
        omnistream::VectorBatch* outputBatch,
        bool isInner);
    void insertLeftVarchar(
        int colIdx,
        std::vector<RowData*>* leftElements,
        std::vector<RowData*>* rightElements,
        omnistream::VectorBatch* outputBatch,
        bool isInner);
    void insertRightVarchar(
        int colIdx,
        std::vector<RowData*>* leftElements,
        std::vector<RowData*>* rightElements,
        omnistream::VectorBatch* outputBatch,
        bool isInner);

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
        omnistream::VectorBatch* outputBatch);
    void BuildInnerRight(
        std::vector<RowData*>* leftElements,
        std::vector<RowData*>* rightElements,
        omnistream::VectorBatch* outputBatch);
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
                case omniruntime::type::DataTypeId::OMNI_INT: filterFuncPtrs.push_back(getValueAddress<int32_t>); break;
                case omniruntime::type::DataTypeId::OMNI_LONG:
                    filterFuncPtrs.push_back(getValueAddress<int64_t>);
                    break;
                case omniruntime::type::DataTypeId::OMNI_DOUBLE:
                    filterFuncPtrs.push_back(getValueAddress<double>);
                    break;
                case omniruntime::type::DataTypeId::OMNI_BOOLEAN:
                    filterFuncPtrs.push_back(getValueAddress<bool>);
                    break;
                default: THROW_LOGIC_EXCEPTION("Type not recognized");
            }
        }
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
    std::vector<RowData*>* leftElements, std::vector<RowData*>* rightElements, omnistream::VectorBatch* outputBatch)
{
    int colIdx = 0;
    // Left side
    for (auto dataType : leftTypes) {
        switch (dataType) {
            case omniruntime::type::DataTypeId::OMNI_SHORT:
                insertLeft<int16_t>(colIdx, leftElements, rightElements, outputBatch, true);
                break;
            case omniruntime::type::DataTypeId::OMNI_INT:
                insertLeft<int32_t>(colIdx, leftElements, rightElements, outputBatch, true);
                break;
            case omniruntime::type::DataTypeId::OMNI_LONG:
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP:
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                insertLeft<int64_t>(colIdx, leftElements, rightElements, outputBatch, true);
                break;
            case omniruntime::type::DataTypeId::OMNI_DOUBLE:
                insertLeft<double>(colIdx, leftElements, rightElements, outputBatch, true);
                break;
            case omniruntime::type::DataTypeId::OMNI_BOOLEAN:
                insertLeft<bool>(colIdx, leftElements, rightElements, outputBatch, true);
                break;
            case omniruntime::type::DataTypeId::OMNI_DECIMAL128:
                insertLeft<omniruntime::type::Decimal128>(colIdx, leftElements, rightElements, outputBatch, true);
                break;
            case omniruntime::type::DataTypeId::OMNI_CHAR:
            case omniruntime::type::DataTypeId::OMNI_VARCHAR:
                insertLeftVarchar(colIdx, leftElements, rightElements, outputBatch, true);
                break;
            default: THROW_LOGIC_EXCEPTION("Type not recognized"); break;
        }
        colIdx++;
    }
}

template <typename KeyType>
void WindowJoinOperator<KeyType>::BuildInnerRight(
    std::vector<RowData*>* leftElements, std::vector<RowData*>* rightElements, omnistream::VectorBatch* outputBatch)
{
    // Right side
    int colIdx = leftTypes.size();
    for (auto dataType : rightTypes) {
        switch (dataType) {
            case omniruntime::type::DataTypeId::OMNI_SHORT:
                insertRight<int16_t>(colIdx, leftElements, rightElements, outputBatch, true);
                break;
            case omniruntime::type::DataTypeId::OMNI_INT:
                insertRight<int32_t>(colIdx, leftElements, rightElements, outputBatch, true);
                break;
            case omniruntime::type::DataTypeId::OMNI_LONG:
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP:
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                insertRight<int64_t>(colIdx, leftElements, rightElements, outputBatch, true);
                break;
            case omniruntime::type::DataTypeId::OMNI_DOUBLE:
                insertRight<double>(colIdx, leftElements, rightElements, outputBatch, true);
                break;
            case omniruntime::type::DataTypeId::OMNI_BOOLEAN:
                insertRight<bool>(colIdx, leftElements, rightElements, outputBatch, true);
                break;
            case omniruntime::type::DataTypeId::OMNI_DECIMAL128:
                insertRight<omniruntime::type::Decimal128>(colIdx, leftElements, rightElements, outputBatch, true);
                break;
            case omniruntime::type::DataTypeId::OMNI_CHAR:
            case omniruntime::type::DataTypeId::OMNI_VARCHAR:
                insertRightVarchar(colIdx, leftElements, rightElements, outputBatch, true);
                break;
            default: THROW_LOGIC_EXCEPTION("Type not recognized"); break;
        }
        colIdx++;
    }
}

template <typename KeyType>
void WindowJoinOperator<KeyType>::buildInner(
    std::vector<RowData*>* leftElements, std::vector<RowData*>* rightElements, omnistream::VectorBatch* outputBatch)
{
    BuildInnerLeft(leftElements, rightElements, outputBatch);
    BuildInnerRight(leftElements, rightElements, outputBatch);
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
                insertLeft<int32_t>(colIdx, leftElements, nullptr, outputBatch, false);
                break;
            case omniruntime::type::DataTypeId::OMNI_LONG:
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
                insertRight<int32_t>(colIdx, nullptr, rightElements, outputBatch, false);
                break;
            case omniruntime::type::DataTypeId::OMNI_LONG:
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
            continue;
        }
        // reuseWindowEndTimes_[i] = windowEndTime;
        auto key = keySelector->getKey(batchGuard.get(), i);

        this->setCurrentKey(key);
        auto inputRow = std::unique_ptr<RowData>(batchGuard->extractRowData(i));
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
    bool isInner)
{
    auto num = leftElements->size();
    auto col = reinterpret_cast<omniruntime::vec::Vector<TYPE>*>(outputBatch->Get(colIdx));
    if (isNonEquiCondition || !isInner) {
        int rowIdx = 0;
        for (int j = 0; j < num; j++) {
            TYPE value = getRowValue<TYPE>(leftElements->at(j), colIdx);
            col->SetValue(rowIdx, value);
            rowIdx++;
        }
    } else {
        int rowIdx = 0;
        for (int j = 0; j < num; j++) {
            TYPE value = getRowValue<TYPE>(leftElements->at(j), colIdx);
            for (size_t i = 0; i < rightElements->size(); i++) {
                col->SetValue(i + rowIdx, value);
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
    bool isInner)
{
    using varcharVecType = omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>;
    auto col = reinterpret_cast<varcharVecType*>(outputBatch->Get(colIdx));
    if (isNonEquiCondition || !isInner) {
        int rowIdx = 0;
        for (auto element : *leftElements) {
            std::string_view strView = element->getStringView(colIdx);
            col->SetValue(rowIdx, strView);
            rowIdx++;
        }
    } else {
        int rowIdx = 0;
        auto num = leftElements->size();
        auto keyGroups = std::vector<int32_t>(num);
        auto sequenceNumbers = std::vector<uint32_t>(num);
        auto rowIds = std::vector<int32_t>(num);

        for (int j = 0; j < num; j++) {
            std::string_view strView = (leftElements->at(j))->getStringView(colIdx);
            for (size_t i = 0; i < rightElements->size(); i++) {
                col->SetValue(i + rowIdx, strView);
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
    bool isInner)
{
    auto num = rightElements->size();
    auto col = reinterpret_cast<omniruntime::vec::Vector<TYPE>*>(outputBatch->Get(colIdx));
    if (isNonEquiCondition || !isInner) {
        int rowIdx = 0;
        for (int i = 0; i < num; i++) {
            TYPE value = getRowValue<TYPE>(rightElements->at(i), colIdx - leftTypes.size());
            col->SetValue(rowIdx, value);
            rowIdx++;
        }
    } else {
        for (size_t i = 0; i < rightElements->size(); i++) {
            TYPE value = getRowValue<TYPE>(rightElements->at(i), colIdx - leftTypes.size());
            for (size_t j = 0; j < leftElements->size(); j++) {
                int valIdx = j * rightElements->size() + i;
                col->SetValue(valIdx, value);
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
    bool isInner)
{
    using varcharVecType = omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>;
    auto col = reinterpret_cast<varcharVecType*>(outputBatch->Get(colIdx));
    if (isNonEquiCondition || !isInner) {
        int rowIdx = 0;
        for (auto element : *rightElements) {
            std::string_view value = element->getStringView(colIdx - leftTypes.size());
            col->SetValue(rowIdx, value);
            rowIdx++;
        }
    } else {
        auto num = rightElements->size();
        for (size_t i = 0; i < rightElements->size(); i++) {
            std::string_view value = (rightElements->at(i))->getStringView(colIdx - leftTypes.size());
            for (size_t j = 0; j < leftElements->size(); j++) {
                int valIdx = j * rightElements->size() + i;
                col->SetValue(valIdx, value);
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
    std::vector<uint8_t> nulls(totalNumOfCols, 0);
    bool resultBool = false;

    const int leftColsCount = static_cast<int>(leftTypes.size());

    // Populate vals and nulls directly from RowData
    for (int col : colRefsForNonEquiCondition) {
        if (col < leftColsCount) {
            extractRowValueToFilterArray(leftRow, col, col, vals.data(), nulls.data(), leftTypes[col]);
        } else {
            int rightColIdx = col - leftColsCount;
            extractRowValueToFilterArray(
                rightRow, rightColIdx, col, vals.data(), nulls.data(), rightTypes[rightColIdx]);
        }
    }

    omniruntime::op::ExecutionContext context;
    bool result = generatedFilter(
        vals.data(),
        reinterpret_cast<bool*>(nulls.data()),
        nullptr,
        &resultBool,
        nullptr,
        reinterpret_cast<int64_t>(&context));

    return result;
}

template <typename KeyType>
bool WindowJoinOperator<KeyType>::keyHasFilteredNull(const std::vector<RowData*>* leftRecords)
{
    if (leftRecords == nullptr || leftRecords->empty() || filterNullKeys.empty()) {
        return false;
    }
    // All rows in the same key group share the same join key, so inspecting one row is
    // equivalent to checking the group key (Flink checks the current key as well).
    RowData* sampleRow = leftRecords->front();
    for (size_t i = 0; i < leftKeyIndex.size(); i++) {
        if (i < filterNullKeys.size() && filterNullKeys[i] && sampleRow->isNullAt(leftKeyIndex[i])) {
            return true;
        }
    }
    return false;
}
