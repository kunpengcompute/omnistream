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

#include "StreamingJoinOperator.h"

#include <limits>

#include "state/JoinRecordStateViews.h"
#include "table/runtime/keyselector/KeySelector.h"
#include "table/typeutils/InternalTypeInfo.h"
#include "table/data/util/RowDataUtil.h"

namespace omnistream {
template <typename K>
void StreamingJoinOperator<K>::processBatch(
    omnistream::VectorBatch* input,
    JoinRecordStateView* inputSideStateView,
    JoinRecordStateView* otherSideStateView,
    bool inputIsLeft)
{
    outputRows_.clear();
    outputRowOwners_.clear();
    outputTimestamps_.clear();
    auto inputGuard = std::unique_ptr<omnistream::VectorBatch>(input);
    for (int32_t i = 0; i < input->GetRowCount(); ++i) {
        currentInputRowTimestamp_ = input->getTimestamp(i);
        auto inputRow = std::shared_ptr<RowData>(input->extractRowData(i));
        if (inputRow == nullptr) {
            THROW_RUNTIME_ERROR("Failed to extract input RowData for streaming join");
        }
        processElement(inputRow, inputSideStateView, otherSideStateView, inputIsLeft);
    }

    auto* outputVectorBatch = buildOutputVectorBatch();
    if (outputVectorBatch != nullptr) {
        this->collector->collect(outputVectorBatch);
    }
}

template <typename K>
void StreamingJoinOperator<K>::processElement(
    const std::shared_ptr<RowData>& input,
    JoinRecordStateView* inputSideStateView,
    JoinRecordStateView* otherSideStateView,
    bool inputIsLeft)
{
    bool inputIsOuter = inputIsLeft ? leftIsOuter : rightIsOuter;
    bool otherIsOuter = inputIsLeft ? rightIsOuter : leftIsOuter;
    bool isAccumulateMsg = RowDataUtil::isAccumulateMsg(input->getRowKind());
    auto inputRowKind = input->getRowKind();
    input->setRowKind(RowKind::INSERT);

    auto& keySelector = inputIsLeft ? this->keySelectorLeft_ : this->keySelectorRight_;
    auto key = keySelector->getKey(input.get());
    this->setCurrentKey(key);

    // 1. Find matched rows in the otherSide
    auto associatedRecords = AbstractStreamingJoinOperator<K>::of(input, inputIsLeft, otherSideStateView);

    // 2. Update inputSide state
    if (isAccumulateMsg) {
        if (inputIsOuter) {
            auto* inputSideOuterStateView = reinterpret_cast<OuterJoinRecordStateView*>(inputSideStateView);
            if (associatedRecords.empty()) {
                // send +I[record+null]
                JoinedRowData joinedRowData;
                joinedRowData.setRowKind(RowKind::INSERT);
                outputNullPadding(joinedRowData, input, inputIsLeft);
                inputSideOuterStateView->addRecord(input, 0);
            } else {
                if (otherIsOuter) {
                    auto* otherSideOuterStateView = reinterpret_cast<OuterJoinRecordStateView*>(otherSideStateView);
                    for (const auto& [other, numOfAssociation] : associatedRecords) {
                        if (numOfAssociation == 0) {
                            // send -D[null+other]
                            JoinedRowData joinedRowData;
                            joinedRowData.setRowKind(RowKind::DELETE);
                            outputNullPadding(joinedRowData, other, !inputIsLeft);
                        }
                        // otherState.update(other, old + 1)
                        otherSideOuterStateView->updateNumOfAssociations(other, numOfAssociation + 1);
                    }
                }
                // send +I[record+other]s
                for (const auto& [other, numOfAssociation] : associatedRecords) {
                    JoinedRowData joinedRowData;
                    joinedRowData.setRowKind(RowKind::INSERT);
                    outputNormally(joinedRowData, input, other, inputIsLeft);
                }
                // state.add(record, other.size)
                inputSideOuterStateView->addRecord(input, associatedRecords.size());
            }
        } else { // input side not outer
            inputSideStateView->addRecord(input);
            if (!associatedRecords.empty()) { // if there are matched rows on the other side
                RowKind outputRowKind;
                if (otherIsOuter) {
                    auto* otherSideOuterStateView = reinterpret_cast<OuterJoinRecordStateView*>(otherSideStateView);
                    for (const auto& [other, numOfAssociation] : associatedRecords) {
                        if (numOfAssociation == 0) {
                            // send -D[null+other]
                            JoinedRowData joinedRowData;
                            joinedRowData.setRowKind(RowKind::DELETE);
                            outputNullPadding(joinedRowData, other, !inputIsLeft);
                        }
                        // otherState.update(other, old + 1)
                        otherSideOuterStateView->updateNumOfAssociations(other, numOfAssociation + 1);
                    }
                    // send +I[record+other]s
                    outputRowKind = RowKind::INSERT;
                } else {
                    // send +I/+U[record+other]s (using input RowKind)
                    outputRowKind = inputRowKind;
                }
                for (const auto& [other, numOfAssociation] : associatedRecords) {
                    JoinedRowData joinedRowData;
                    joinedRowData.setRowKind(outputRowKind);
                    outputNormally(joinedRowData, input, other, inputIsLeft);
                }
            }
            // skip when there is no matched rows on the other side
        }
    } else { // input record is retract
        inputSideStateView->retractRecord(input);
        if (associatedRecords.empty()) { // there is no matched rows on the other side
            if (inputIsOuter) {          // input side is outer
                // send -D[record+null]
                JoinedRowData joinedRowData;
                joinedRowData.setRowKind(RowKind::DELETE);
                outputNullPadding(joinedRowData, input, inputIsLeft);
            }
            // nothing to do when input side is not outer
        } else { // there are matched rows on the other side
            RowKind outputRowKind;
            if (inputIsOuter) {
                // send -D[record+other]s
                outputRowKind = RowKind::DELETE;
            } else {
                // send -D/-U[record+other]s (using input RowKind)
                outputRowKind = inputRowKind;
            }
            for (const auto& [other, numOfAssociation] : associatedRecords) {
                JoinedRowData joinedRowData;
                joinedRowData.setRowKind(outputRowKind);
                outputNormally(joinedRowData, input, other, inputIsLeft);
            }
            // if other side is outer
            if (otherIsOuter) {
                auto* otherSideOuterStateView = reinterpret_cast<OuterJoinRecordStateView*>(otherSideStateView);
                for (const auto& [other, numOfAssociation] : associatedRecords) {
                    if (numOfAssociation == 1) {
                        // send +I[null+other]
                        JoinedRowData joinedRowData;
                        joinedRowData.setRowKind(RowKind::INSERT);
                        outputNullPadding(joinedRowData, other, !inputIsLeft);
                    } // nothing else to do when number of associations > 1
                    // otherState.update(other, old - 1)
                    otherSideOuterStateView->updateNumOfAssociations(other, numOfAssociation - 1);
                }
            }
        }
    }

    if constexpr (KeyTypeTraits<K>::isRowKey) {
        delete key;
    }
}

template <typename K>
void StreamingJoinOperator<K>::open()
{
    AbstractStreamingJoinOperator<K>::open();
    auto* leftRecordType = InternalTypeInfo::ofRowType(
        new omnistream::RowType(true, this->description["leftInputTypes"].template get<std::vector<std::string>>()));
    auto* rightRecordType = InternalTypeInfo::ofRowType(
        new omnistream::RowType(true, this->description["rightInputTypes"].template get<std::vector<std::string>>()));
    if (leftIsOuter) {
        std::string stateName = "left-records";
        leftRecordStateView_ =
            OuterJoinRecordStateViews::create<K>(this->getRuntimeContext(), stateName, leftRecordType);
    } else {
        std::string stateName = "left-records";
        leftRecordStateView_ = JoinRecordStateViews::create(this->getRuntimeContext(), stateName, leftRecordType);
    }
    if (rightIsOuter) {
        NOT_IMPL_EXCEPTION;
    } else {
        std::string stateName = "right-records";
        rightRecordStateView_ = JoinRecordStateViews::create(this->getRuntimeContext(), stateName, rightRecordType);
    }
    std::vector<int> leftKeyTypes;
    std::vector<int> rightKeyTypes;
    for (auto kIndex : this->leftKeyIndex) {
        leftKeyTypes.push_back(this->leftInputTypes[kIndex]);
    }
    for (auto kIndex : this->rightKeyIndex) {
        rightKeyTypes.push_back(this->rightInputTypes[kIndex]);
    }
    // make sure the key types are the same
    if (leftKeyTypes != rightKeyTypes) {
        throw std::runtime_error("Left key types do not match right key types");
    }

    this->keySelectorLeft_ = std::make_unique<KeySelector<K>>(leftKeyTypes, this->leftKeyIndex);
    this->keySelectorRight_ = std::make_unique<KeySelector<K>>(rightKeyTypes, this->rightKeyIndex);

    this->leftNullRow_ = std::make_unique<GenericRowData>(this->leftArity_);
    this->rightNullRow_ = std::make_unique<GenericRowData>(this->rightArity_);
}

template class StreamingJoinOperator<RowData*>;
template class StreamingJoinOperator<long>;
} // namespace omnistream
