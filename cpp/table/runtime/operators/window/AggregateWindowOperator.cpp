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

#include <memory>
#include "AggregateWindowOperator.h"
#include "table/runtime/generated/function/GroupWindowAggsHandleFunction.h"
#include "table/runtime/generated/NamespaceAggsBasicFunctionFactory.h"

template <typename K, typename W>
std::unique_ptr<NamespaceAggsHandleFunction<W>> AggregateWindowOperator<K, W>::initNamespaceAggsHandleFunction(
    const nlohmann::json& aggInfoList)
{
    // TODO: namespace is unused in functions, which may cause WindowOperator only supports session window(each key
    // corresponds to only one window)
    auto aggregateCalls = aggInfoList["aggregateCalls"].get<vector<nlohmann::json>>();
    auto accTypes = aggInfoList["AccTypes"].get<vector<std::string>>();
    accTypes.erase(
        std::remove_if(
            accTypes.begin(),
            accTypes.end(),
            [](const std::string& type) { return type.find("RAW") != std::string::npos; }),
        accTypes.end());
    std::vector<int32_t> accTypeIds;
    for (const auto& type : accTypes) {
        accTypeIds.push_back(LogicalType::flinkTypeToOmniTypeId(type));
    }
    auto aggValueTypes = aggInfoList["aggValueTypes"].get<vector<std::string>>();
    aggValueTypes.erase(
        std::remove_if(
            aggValueTypes.begin(),
            aggValueTypes.end(),
            [](const std::string& type) { return type.find("RAW") != std::string::npos; }),
        aggValueTypes.end());
    std::vector<int32_t> aggValueTypeIds;
    for (const auto& type : aggValueTypes) {
        aggValueTypeIds.push_back(LogicalType::flinkTypeToOmniTypeId(type));
    }
    const int32_t indexOfCountStar = aggInfoList["indexOfCountStar"].get<int32_t>();
    const bool countStarInserted = aggInfoList.value("countStarInserted", false);

    // TODO: namespace currently only supports TimeWindow
    std::vector<std::unique_ptr<NamespaceAggsBasicFunction<W>>> functions;
    auto accStartIndex = 0;
    auto aggValueStartIndex = 0;
    for (const auto& aggregateCall : aggregateCalls) {
        std::string aggTypeStr = aggregateCall["name"];
        const int32_t filterIndex = aggregateCall["filterArg"].get<int32_t>();    // TODO: not support now
        const std::string consumeRetraction = aggregateCall["consumeRetraction"]; // TODO: not support now

        const auto argIndexes = aggregateCall.value("argIndexes", std::vector<int32_t>{});
        const bool isInsertedCountStar = countStarInserted && accStartIndex == indexOfCountStar;
        const int32_t aggValueIndex = isInsertedCountStar ? -1 : aggValueStartIndex;
        const int32_t aggValueTypeId = isInsertedCountStar ? -1 : aggValueTypeIds[aggValueIndex];
        auto accIndexes = NamespaceAggsBasicFunctionFactory::getAccIndexes(aggTypeStr, accStartIndex);
        functions.push_back(
            NamespaceAggsBasicFunctionFactory::create<W>(
                aggTypeStr, argIndexes, this->inputTypeIds_, accIndexes, accTypeIds, aggValueIndex, aggValueTypeId));
        accStartIndex += accIndexes.size();
        if (!isInsertedCountStar) {
            aggValueStartIndex++;
        }
    }
    return std::make_unique<GroupWindowAggsHandleFunction<W>>(
        std::move(functions),
        std::move(aggValueTypeIds),
        this->windowPropertyTypeIds_,
        std::vector<int32_t>(this->outputTypeIds.begin() + this->keyedTypes.size(), this->outputTypeIds.end()),
        this->accumulatorArity,
        this->shiftTimeZone);
}

template <typename K, typename W>
omnistream::VectorBatch* AggregateWindowOperator<K, W>::createOutputBatch(const std::vector<RowData*>& collectedRows)
{
    int numColumns = WindowOperator<K, W>::outputTypes.size();
    const int numRows = static_cast<int>(collectedRows.size()); // Number of rows collected

    // Create a new VectorBatch (empty if no rows exist)
    auto* outputBatch = new omnistream::VectorBatch(numRows);

    // Loop through each column and create vectors
    for (int colIndex = 0; colIndex < numColumns; ++colIndex) {
        switch (outputTypeIds[colIndex]) {
            case OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case OMNI_TIMESTAMP:
            case OMNI_LONG: {
                auto* vector = new omniruntime::vec::Vector<int64_t>(numRows);
                for (int rowIndex = 0; rowIndex < numRows; ++rowIndex) {
                    if (collectedRows[rowIndex]->isNullAt(colIndex)) {
                        vector->SetNull(rowIndex);
                    } else {
                        vector->SetValue(rowIndex, *collectedRows[rowIndex]->getLong(colIndex));
                    }
                }
                outputBatch->Append(vector);
                break;
            }
            case OMNI_INT: {
                auto* vector = new omniruntime::vec::Vector<int32_t>(numRows);
                for (int rowIndex = 0; rowIndex < numRows; ++rowIndex) {
                    if (collectedRows[rowIndex]->isNullAt(colIndex)) {
                        vector->SetNull(rowIndex);
                    } else {
                        vector->SetValue(rowIndex, *collectedRows[rowIndex]->getInt(colIndex));
                    }
                }
                outputBatch->Append(vector);
                break;
            }
            case OMNI_DOUBLE: {
                auto* vector = new omniruntime::vec::Vector<double>(numRows);
                for (int rowIndex = 0; rowIndex < numRows; ++rowIndex) {
                    if (collectedRows[rowIndex]->isNullAt(colIndex)) {
                        vector->SetNull(rowIndex);
                    } else {
                        vector->SetValue(rowIndex, *collectedRows[rowIndex]->getLong(colIndex));
                    }
                }
                outputBatch->Append(vector);
                break;
            }
            case OMNI_BOOLEAN: {
                auto* vector = new omniruntime::vec::Vector<bool>(numRows);
                for (int rowIndex = 0; rowIndex < numRows; ++rowIndex) {
                    if (collectedRows[rowIndex]->isNullAt(colIndex)) {
                        vector->SetNull(rowIndex);
                    } else {
                        vector->SetValue(rowIndex, *collectedRows[rowIndex]->getInt(colIndex));
                    }
                }
                outputBatch->Append(vector);
                break;
            }
            case OMNI_VARCHAR: {
                auto* vector =
                    new omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>(numRows);
                for (int rowIndex = 0; rowIndex < numRows; ++rowIndex) {
                    if (collectedRows[rowIndex]->isNullAt(colIndex)) {
                        vector->SetNull(rowIndex);
                    } else {
                        std::string_view strView = collectedRows[rowIndex]->getStringView(colIndex);
                        vector->SetValue(rowIndex, strView);
                    }
                }
                outputBatch->Append(vector);
                break;
            }
            default: {
                THROW_RUNTIME_ERROR("Unsupported column type in inputRow");
            }
        }
    }

    // Set row kind for all rows (only if there are rows)
    for (int rowIndex = 0; rowIndex < numRows; ++rowIndex) {
        outputBatch->setRowKind(rowIndex, collectedRows[rowIndex]->getRowKind());
    }
    return outputBatch;
}

template <typename K, typename W>
void AggregateWindowOperator<K, W>::emitWindowResult(const W& window)
{
    this->windowFunction->prepareAggregateAccumulatorForEmit(window);
    auto aggResult = std::unique_ptr<RowData>(this->windowAggregator->getValue(window));

    if (this->produceUpdates_) {
        NOT_IMPL_EXCEPTION;
    } else {
        if (aggResult != nullptr) {
            // send INSERT
            if constexpr (KeySelector<K>::isSharedRowKey_) {
                collect(RowKind::INSERT, this->stateHandler->getCurrentKey().get(), std::move(aggResult));
            } else {
                NOT_IMPL_EXCEPTION;
            }
        }
        // if the counter is zero, no need to send accumulate
        // there is no possible skip `if` branch when `produceUpdates` is false
    }
    if (this->shouldDeleteWindowStateValue()) {
        delete this->windowAggregator->getAccumulators();
    }
}

template class AggregateWindowOperator<std::shared_ptr<RowData>, TimeWindow>;
