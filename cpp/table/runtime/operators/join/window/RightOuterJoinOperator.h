/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2024. All rights reserved.
 */

#ifndef OMNISTREAM_RIGHTOUTERJOINOPERATOR_H
#define OMNISTREAM_RIGHTOUTERJOINOPERATOR_H
#include "InnerJoinOperator.h"

template <typename KeyType>
class RightOuterJoinOperator : public AbstractOuterJoinOperator<KeyType> {
public:
    RightOuterJoinOperator(
        const nlohmann::json& config, Output* output, TypeSerializer* leftSerializer, TypeSerializer* rightSerializer);
    void join(std::vector<RowData*>* leftRecords, std::vector<RowData*>* rightRecords) override;
};

template <typename KeyType>
RightOuterJoinOperator<KeyType>::RightOuterJoinOperator(
    const nlohmann::json& config, Output* output, TypeSerializer* leftSerializer, TypeSerializer* rightSerializer)
    : AbstractOuterJoinOperator<KeyType>(config, output, leftSerializer, rightSerializer)
{
}

template <typename KeyType>
void RightOuterJoinOperator<KeyType>::join(std::vector<RowData*>* leftRecords, std::vector<RowData*>* rightRecords)
{
    // 1. right side has no records: no output.
    if (rightRecords == nullptr || rightRecords->empty()) {
        return;
    }
    // 2. left side has no records: pad null left columns for all right rows.
    if (leftRecords == nullptr || leftRecords->empty()) {
        auto outputBatch = omnistream::VectorBatch::CreateVectorBatch(rightRecords->size(), this->outputTypes);
        this->buildLeftNull(rightRecords, outputBatch);
        AbstractStreamOperator<KeyType>::output->collect(outputBatch);
        return;
    }
    // Non-equi per-right emission (matches Flink: a right record's matched pairs are emitted
    // together, followed by its null-padded row when it matches nothing).
    auto emitNonEqui = [this](std::vector<RowData*>* lefts, std::vector<RowData*>* rights) {
        std::vector<RowData*> matchedLeft;
        std::vector<RowData*> matchedRight;
        std::vector<RowData*> singleRight(1, nullptr);
        for (auto rightRecord : *rights) {
            matchedLeft.clear();
            for (auto leftRecord : *lefts) {
                if (this->filter(static_cast<RowData*>(leftRecord), static_cast<RowData*>(rightRecord))) {
                    matchedLeft.push_back(leftRecord);
                }
            }
            if (!matchedLeft.empty()) {
                // buildInner's non-equi path fills output rows sequentially from parallel
                // left/right arrays (row i = left[i] + right[i]), so repeat the right record
                // once per matched left record.
                matchedRight.assign(matchedLeft.size(), rightRecord);
                auto outputBatch = omnistream::VectorBatch::CreateVectorBatch(matchedLeft.size(), this->outputTypes);
                this->buildInner(&matchedLeft, &matchedRight, outputBatch);
                AbstractStreamOperator<KeyType>::output->collect(outputBatch);
            } else {
                singleRight[0] = rightRecord;
                auto outputBatch = omnistream::VectorBatch::CreateVectorBatch(1, this->outputTypes);
                this->buildLeftNull(&singleRight, outputBatch);
                AbstractStreamOperator<KeyType>::output->collect(outputBatch);
            }
        }
    };
    // 3. No filtered null key on either side: the rows share the same (non-null) join key, so
    //    the fast paths apply. (Row keys keep a null group on this path when filterNullKeys is
    //    all-false, mirroring Flink; scalar keys reach here only when no row has an actual null
    //    key, since the scalar key cannot carry the null flag.)
    if (!this->hasAnyFilteredNull(leftRecords, true) && !this->hasAnyFilteredNull(rightRecords, false)) {
        // 3a. equi condition: rows in the same key group and same window share equal join keys,
        //     so the cartesian product is the set of matched pairs. Emit in right-major order to
        //     match Flink's RightOuterJoinOperator (right records in the outer loop).
        if (!this->isNonEquiCondition) {
            auto outputBatch =
                omnistream::VectorBatch::CreateVectorBatch(leftRecords->size() * rightRecords->size(), this->outputTypes);
            this->buildInner(leftRecords, rightRecords, outputBatch, true);
            AbstractStreamOperator<KeyType>::output->collect(outputBatch);
            return;
        }
        // 3b. non-equi condition: per-right emission.
        emitNonEqui(leftRecords, rightRecords);
        return;
    }
    // 4. Mixed / null-key group: split the group into the null-key subgroup (rows can never
    //    match) and the non-null subgroup (standard right-outer semantics). For scalar keys any
    //    actual null key counts as filtered (the key cannot carry the null flag and the row would
    //    collapse into an arbitrary group); for row keys only planner-declared filtered keys do.
    //    This mirrors Flink, where null-key rows live in their own key group and are filtered by
    //    JoinConditionWithNullFilters.
    std::vector<RowData*> leftNull;
    std::vector<RowData*> leftOk;
    std::vector<RowData*> rightNull;
    std::vector<RowData*> rightOk;
    for (auto leftRecord : *leftRecords) {
        (this->hasFilteredNullKey(static_cast<RowData*>(leftRecord), true) ? leftNull : leftOk).push_back(leftRecord);
    }
    for (auto rightRecord : *rightRecords) {
        (this->hasFilteredNullKey(static_cast<RowData*>(rightRecord), false) ? rightNull : rightOk).push_back(rightRecord);
    }
    // 4a. null-key right rows can never match any row -> left-null-pad them.
    if (!rightNull.empty()) {
        auto outputBatch = omnistream::VectorBatch::CreateVectorBatch(rightNull.size(), this->outputTypes);
        this->buildLeftNull(&rightNull, outputBatch);
        AbstractStreamOperator<KeyType>::output->collect(outputBatch);
    }
    // 4b. non-null subgroup: standard right-outer semantics.
    if (leftOk.empty()) {
        if (!rightOk.empty()) {
            auto outputBatch = omnistream::VectorBatch::CreateVectorBatch(rightOk.size(), this->outputTypes);
            this->buildLeftNull(&rightOk, outputBatch);
            AbstractStreamOperator<KeyType>::output->collect(outputBatch);
        }
    } else if (!rightOk.empty()) {
        if (!this->isNonEquiCondition) {
            auto outputBatch =
                omnistream::VectorBatch::CreateVectorBatch(leftOk.size() * rightOk.size(), this->outputTypes);
            this->buildInner(&leftOk, &rightOk, outputBatch, true);
            AbstractStreamOperator<KeyType>::output->collect(outputBatch);
        } else {
            emitNonEqui(&leftOk, &rightOk);
        }
    }
    // 4c. null-key left rows are never emitted by a right-outer join (they can never match),
    //     matching Flink's behaviour for the null-key group.
}

#endif // OMNISTREAM_RIGHTOUTERJOINOPERATOR_H
