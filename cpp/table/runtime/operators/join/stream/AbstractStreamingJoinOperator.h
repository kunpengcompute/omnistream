/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * omnistream is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan
 * PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *         http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 */

#pragma once

#include "table/data/RowData.h"
#include "state/JoinRecordStateViews.h"
#include "state/OuterJoinRecordStateViews.h"

#include "streaming/api/operators/AbstractStreamOperator.h"
#include "streaming/api/operators/TimestampedCollector.h"
#include "expression/expr_printer.h"
#include "streaming/api/operators/TwoInputStreamOperator.h"
#include "OmniOperatorJIT/core/src/expression/jsonparser/jsonparser.h"

#include "OmniOperatorJIT/core/src/codegen/simple_filter_codegen.h"
#include "OmniOperatorJIT/core/src/operator/execution_context.h"
#include "state/JoinRecordStateView.h"
#include "runtime/keyselector/KeySelector.h"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <set>
#include <tuple>
#include <vector>

// joinCondition includes 2 steps:
// (1) check if key is null
// (2) check if filter condition is satisfied
using namespace omniruntime::expressions;

using FilterFuncPtr = bool (*)(int64_t*, bool*, int32_t*, bool*, int32_t*, int64_t);
using RowDataFilterFieldGetter = void (*)(RowData*, int32_t, int32_t, int64_t*);
using JoinedRowFilterFunc = std::vector<RowDataFilterFieldGetter>;

template <typename TYPE>
void getValueAddress(
    omniruntime::vec::BaseVector* vec, int32_t rowId, int32_t colId, int64_t* valuesPtr, bool* isNullPtr)
{
    omniruntime::vec::Vector<TYPE>* castedVec = reinterpret_cast<omniruntime::vec::Vector<TYPE>*>(vec);
    valuesPtr[colId] =
        reinterpret_cast<int64_t>(omniruntime::vec::unsafe::UnsafeVector::GetRawValues<TYPE>(castedVec)) +
        rowId * sizeof(TYPE);
    isNullPtr[colId] = castedVec->IsNull(rowId);
}

template <typename TYPE>
void getRowDataValueAddress(RowData* row, int32_t rowCol, int32_t outputCol, int64_t* valuesPtr)
{
    valuesPtr[outputCol] = reinterpret_cast<int64_t>(row->getLong(rowCol));
}

template <>
inline void getRowDataValueAddress<int32_t>(RowData* row, int32_t rowCol, int32_t outputCol, int64_t* valuesPtr)
{
    valuesPtr[outputCol] = reinterpret_cast<int64_t>(row->getInt(rowCol));
}

template <>
inline void getRowDataValueAddress<bool>(RowData* row, int32_t rowCol, int32_t outputCol, int64_t* valuesPtr)
{
    valuesPtr[outputCol] = reinterpret_cast<int64_t>(row->getBool(rowCol));
}

template <typename K>
class AbstractStreamingJoinOperator : public AbstractStreamOperator<K>, public TwoInputStreamOperator {
public:
    AbstractStreamingJoinOperator(const nlohmann::json& description, Output* output);

    void processWatermarkStatus1(WatermarkStatus* status) override
    {
        AbstractStreamOperator<K>::processWatermarkStatus1(status);
    }

    void processWatermarkStatus2(WatermarkStatus* status) override
    {
        AbstractStreamOperator<K>::processWatermarkStatus2(status);
    }

    ~AbstractStreamingJoinOperator() override
    {
        LOG("AbstractStreamingJoinOperator<K>::~AbstractStreamingJoinOperator");
    };

    void open() override;
    void close() override;
    void setKeyContextElement1(StreamRecord* record) override;
    void setKeyContextElement2(StreamRecord* record) override;
    void initializeState(StreamTaskStateInitializerImpl* initializer, TypeSerializer* keySerializer) override;

    void notifyCheckpointComplete(long checkpointId) override;

    void notifyCheckpointAborted(long checkpointId) override;

    bool isSetKeyContextElement1() override
    {
        return true;
    }
    bool isSetKeyContextElement2() override
    {
        return true;
    }

    std::string getTypeName() override
    {
        std::string typeName = "AbstractStreamingJoinOperator";
        typeName.append(__PRETTY_FUNCTION__);
        return typeName;
    }

    // Find matched records
    std::vector<std::tuple<shared_ptr<RowData>, int32_t>> of(
        const std::shared_ptr<RowData>& input, bool inputIsLeft, omnistream::JoinRecordStateView* otherSideStateView);

protected:
    std::string leftInputSpec;
    std::string rightInputSpec;

    std::vector<bool> filterNullKeys;

    long leftStateRetentionTime = 0;
    long rightStateRetentionTime = 0;

    TimestampedCollector* collector;

    // The description we get from RexNode
    nlohmann::json description;

    std::vector<int32_t> leftKeyIndex;
    std::vector<int32_t> rightKeyIndex;

    std::unique_ptr<KeySelector<K>> keySelectorLeft_;
    std::unique_ptr<KeySelector<K>> keySelectorRight_;
    // std::vector<std::vector<int32_t>> Now we only consider one composite key (the first one).
    std::vector<int32_t> leftUniqueKeyIndex;
    std::vector<int32_t> rightUniqueKeyIndex;

    std::vector<int32_t> leftInputTypes;
    size_t leftArity_;
    std::vector<int32_t> rightInputTypes;
    size_t rightArity_;

    FilterFuncPtr generatedFilter = nullptr;
    JoinedRowFilterFunc joinCondition;
    std::vector<int64_t> reUsableVals_;
    std::vector<int8_t> reUsableNulls_; // reinterpreted as bool when used

    std::set<int> colRefsForNonEquiCondition;
    std::set<int> getColRefs(nlohmann::json& config);

    bool filterRecord(
        const std::shared_ptr<RowData>& inputRow, const std::shared_ptr<RowData>& otherRow, bool inputIsLeft);

    bool isJoinKeyFiltered(const std::shared_ptr<RowData>& inputRow, bool inputIsLeft) const;

private:
    // generatedFilter points into code JIT-compiled inside the codegen's LLVM engine, and
    // CodegenBase only keeps a non-owning pointer to the expression -- so both have to stay alive
    // for as long as the operator may call the filter. Owning them here ties their lifetime to the
    // operator, which the task destroys at the end of each job.
    //
    // Declaration order is load-bearing: members are destroyed in reverse, so the codegen is torn
    // down before the expression it points at.
    std::unique_ptr<omniruntime::expressions::Expr> nonEquiConditionExpr_;
    std::unique_ptr<SimpleFilterCodeGen> nonEquiFilterCodegen_;

    JoinedRowFilterFunc generateJoinFilterFunction(const nlohmann::json& description)
    {
        JoinedRowFilterFunc filterFuncPtrs;

        reUsableVals_.resize(leftArity_ + rightArity_);
        reUsableNulls_.resize(leftArity_ + rightArity_);

        if (description.contains("nonEquiCondition") && !description["nonEquiCondition"].is_null()) {
            auto filter = description["nonEquiCondition"];
            nonEquiConditionExpr_.reset(JSONParser::ParseJSON(filter));
            nonEquiFilterCodegen_ =
                std::make_unique<SimpleFilterCodeGen>("nonEquiCondition", *nonEquiConditionExpr_, nullptr);
            int64_t filterAddress = nonEquiFilterCodegen_->GetFunction();
            generatedFilter = *static_cast<FilterFuncPtr*>(reinterpret_cast<void*>(&filterAddress));

            colRefsForNonEquiCondition = getColRefs(filter);

            for (size_t i = 0; i < description["outputTypes"].size(); i++) {
                if (colRefsForNonEquiCondition.find(i) == colRefsForNonEquiCondition.end()) {
                    filterFuncPtrs.push_back(nullptr);
                } else {
                    bool leftSideState = i < leftInputTypes.size();
                    switch (leftSideState ? leftInputTypes[i] : rightInputTypes[i - leftInputTypes.size()]) {
                        case omniruntime::type::DataTypeId::OMNI_SHORT:
                            filterFuncPtrs.push_back(getRowDataValueAddress<int16_t>);
                            break;
                        case omniruntime::type::DataTypeId::OMNI_INT:
                            filterFuncPtrs.push_back(getRowDataValueAddress<int32_t>);
                            break;
                        case omniruntime::type::DataTypeId::OMNI_LONG:
                        case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
                        case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                        case omniruntime::type::DataTypeId::OMNI_TIMESTAMP:
                            filterFuncPtrs.push_back(getRowDataValueAddress<int64_t>);
                            break;
                        case omniruntime::type::DataTypeId::OMNI_DOUBLE:
                            filterFuncPtrs.push_back(getRowDataValueAddress<double>);
                            break;
                        case omniruntime::type::DataTypeId::OMNI_BOOLEAN:
                            filterFuncPtrs.push_back(getRowDataValueAddress<bool>);
                            break;
                        default: THROW_LOGIC_EXCEPTION("Type not recognized"); break;
                    }
                }
            }
        } else {
            // Add other join filters
            WARN_RELEASE("no JoinFilter or not supported JoinFilter: ");
        }

        return filterFuncPtrs;
    };
};

template <typename K>
void AbstractStreamingJoinOperator<K>::open()
{
    try {
        AbstractStreamOperator<K>::open();
        joinCondition = generateJoinFilterFunction(description);
    } catch (const std::runtime_error& e) {
        throw std::runtime_error("failed to open join operator");
    }
    if (leftKeyIndex.size() != rightKeyIndex.size()) {
        throw std::runtime_error("leftKeyIndex size does not match rightKeyIndex size");
    }
}
template <typename K>
void AbstractStreamingJoinOperator<K>::close()
{
    AbstractStreamOperator<K>::close();
}

template <typename K>
void AbstractStreamingJoinOperator<K>::setKeyContextElement1(StreamRecord* record)
{
}

template <typename K>
void AbstractStreamingJoinOperator<K>::setKeyContextElement2(StreamRecord* record)
{
}
template <typename K>
void AbstractStreamingJoinOperator<K>::initializeState(
    StreamTaskStateInitializerImpl* initializer, TypeSerializer* keySerializer)
{
    AbstractStreamOperator<K>::SetOperatorID(TwoInputStreamOperator::GetOperatorID().toString());
    AbstractStreamOperator<K>::initializeState(initializer, keySerializer);
}

template <typename K>
void AbstractStreamingJoinOperator<K>::notifyCheckpointComplete(long checkpointId)
{
    AbstractStreamOperator<K>::notifyCheckpointComplete(checkpointId);
}

template <typename K>
void AbstractStreamingJoinOperator<K>::notifyCheckpointAborted(long checkpointId)
{
    AbstractStreamOperator<K>::notifyCheckpointAborted(checkpointId);
}

template <typename K>
std::vector<std::tuple<std::shared_ptr<RowData>, int32_t>> AbstractStreamingJoinOperator<K>::of(
    const std::shared_ptr<RowData>& input, bool inputIsLeft, omnistream::JoinRecordStateView* otherSideStateView)
{
    std::vector<std::tuple<std::shared_ptr<RowData>, int32_t>> associations{};
    if (isJoinKeyFiltered(input, inputIsLeft)) {
        return associations;
    }
    if (omnistream::JoinRecordStateView::isOuterJoinRecordStateViewType(
            otherSideStateView->getJoinRecordStateViewType())) {
        auto castedView = reinterpret_cast<omnistream::OuterJoinRecordStateView*>(otherSideStateView);
        auto records = castedView->getRecordsAndNumOfAssociations();

        while (records->hasNext()) {
            auto record = records->next();
            if (joinCondition.empty() || filterRecord(input, std::get<0>(record), inputIsLeft)) {
                associations.push_back(record);
            }
        }
    } else {
        auto records = otherSideStateView->getRecords();
        while (records->hasNext()) {
            auto record = records->next();
            if (joinCondition.empty() || filterRecord(input, record, inputIsLeft)) {
                associations.emplace_back(record, -1);
            }
        }
    }
    return associations;
}

template <typename K>
AbstractStreamingJoinOperator<K>::AbstractStreamingJoinOperator(const nlohmann::json& description, Output* output)
{
    this->description = description;
    // parse description to get left/right dataTypeId
    for (const auto& typeStr : description["leftInputTypes"].get<std::vector<std::string>>()) {
        leftInputTypes.push_back(LogicalType::flinkTypeToOmniTypeId(typeStr));
    }
    for (const auto& typeStr : description["rightInputTypes"].get<std::vector<std::string>>()) {
        rightInputTypes.push_back(LogicalType::flinkTypeToOmniTypeId(typeStr));
    }
    leftArity_ = leftInputTypes.size();
    rightArity_ = rightInputTypes.size();

    // parse description to get parameters
    rightKeyIndex = description["rightJoinKey"].get<std::vector<int32_t>>();
    leftKeyIndex = description["leftJoinKey"].get<std::vector<int32_t>>();
    filterNullKeys = description["filterNulls"].get<std::vector<bool>>();

    auto getFirstArray = [](const nlohmann::json& jsonObject, const std::string& key) -> std::vector<int> {
        if (jsonObject.contains(key) && jsonObject[key].is_array() && !jsonObject[key].empty() &&
            jsonObject[key][0].is_array()) {
            return jsonObject[key][0].get<std::vector<int>>();
        }
        return {}; // Return an empty vector if conditions are not met
    };
    leftUniqueKeyIndex = getFirstArray(description, "leftUniqueKeys");
    rightUniqueKeyIndex = getFirstArray(description, "rightUniqueKeys");

    leftInputSpec = description["leftInputSpec"];
    rightInputSpec = description["rightInputSpec"];

    // Initialize collector
    this->output = output;
    this->collector = new TimestampedCollector(this->output);
}

template <typename K>
std::set<int> AbstractStreamingJoinOperator<K>::getColRefs(nlohmann::json& config)
{
    std::set<int> colRefs;

    if (config["exprType"] == "FIELD_REFERENCE") {
        colRefs.emplace(config["colVal"]);
    }

    if (config.contains("right")) {
        auto rightColRefs = getColRefs(config["right"]);
        colRefs.insert(rightColRefs.begin(), rightColRefs.end());
    }

    if (config.contains("left")) {
        auto leftColRefs = getColRefs(config["left"]);
        colRefs.insert(leftColRefs.begin(), leftColRefs.end());
    }

    return colRefs;
}

template <typename K>
bool AbstractStreamingJoinOperator<K>::filterRecord(
    const std::shared_ptr<RowData>& inputRow, const std::shared_ptr<RowData>& otherRow, bool inputIsLeft)
{
    if (inputRow == nullptr) {
        THROW_RUNTIME_ERROR("input RowData is nullptr");
    }
    std::fill(reUsableVals_.begin(), reUsableVals_.end(), 0);
    std::fill(reUsableNulls_.begin(), reUsableNulls_.end(), 0);
    bool resultBool; // todo: Unknown purpose

    // for the inputSide
    for (auto col : colRefsForNonEquiCondition) {
        bool isLeftColumn = col < leftArity_;
        if ((inputIsLeft && isLeftColumn) || (!inputIsLeft && !isLeftColumn)) {
            auto localCol = static_cast<int32_t>(inputIsLeft ? col : col - leftArity_);
            reUsableNulls_[col] = static_cast<int8_t>(inputRow->isNullAt(localCol));
            if (reUsableNulls_[col]) {
                reUsableVals_[col] = 0;
            } else {
                if (joinCondition[col] == nullptr) {
                    THROW_RUNTIME_ERROR("Missing RowData field getter for non-equi join condition");
                }
                joinCondition[col](inputRow.get(), localCol, col, reUsableVals_.data());
            }
        }
    }
    // for the otherSide
    for (auto col : colRefsForNonEquiCondition) {
        bool isLeftColumn = col < leftArity_;
        if ((inputIsLeft && !isLeftColumn) || (!inputIsLeft && isLeftColumn)) {
            auto localCol = static_cast<int32_t>(inputIsLeft ? col - leftArity_ : col);
            reUsableNulls_[col] = static_cast<int8_t>(otherRow->isNullAt(localCol));
            if (reUsableNulls_[col]) {
                reUsableVals_[col] = 0;
            } else {
                if (joinCondition[col] == nullptr) {
                    THROW_RUNTIME_ERROR("Missing RowData field getter for non-equi join condition");
                }
                joinCondition[col](otherRow.get(), localCol, col, reUsableVals_.data());
            }
        }
    }

    omniruntime::op::ExecutionContext context;
    if (generatedFilter == nullptr) {
        THROW_RUNTIME_ERROR("Missing generated filter for non-equi join condition");
    }
    return generatedFilter(
        reUsableVals_.data(),
        reinterpret_cast<bool*>(reUsableNulls_.data()),
        nullptr,
        &resultBool,
        nullptr,
        (int64_t)(&context));
}

template <typename K>
bool AbstractStreamingJoinOperator<K>::isJoinKeyFiltered(
    const std::shared_ptr<RowData>& inputRow, bool inputIsLeft) const
{
    if (inputRow == nullptr) {
        THROW_RUNTIME_ERROR("input RowData is nullptr");
    }
    if (filterNullKeys.empty() || !filterNullKeys[0]) {
        return false;
    }

    const auto& keyIndices = inputIsLeft ? leftKeyIndex : rightKeyIndex;
    return std::any_of(
        keyIndices.begin(), keyIndices.end(), [&inputRow](int32_t keyIndex) { return inputRow->isNullAt(keyIndex); });
}
