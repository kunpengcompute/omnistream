/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2024. All rights reserved.
 */

#ifndef OMNISTREAM_LEFTOUTERJOINOPERATOR_H
#define OMNISTREAM_LEFTOUTERJOINOPERATOR_H
#include "InnerJoinOperator.h"

template <typename KeyType>
class LeftOuterJoinOperator : public AbstractOuterJoinOperator<KeyType> {
public:
    LeftOuterJoinOperator(
        const nlohmann::json& config, Output* output, TypeSerializer* leftSerializer, TypeSerializer* rightSerializer);
    void join(std::vector<RowData*>* leftRecords, std::vector<RowData*>* rightRecords) override;
};

template <typename KeyType>
LeftOuterJoinOperator<KeyType>::LeftOuterJoinOperator(
    const nlohmann::json& config, Output* output, TypeSerializer* leftSerializer, TypeSerializer* rightSerializer)
    : AbstractOuterJoinOperator<KeyType>(config, output, leftSerializer, rightSerializer)
{
}

template <typename KeyType>
void LeftOuterJoinOperator<KeyType>::join(std::vector<RowData*>* leftRecords, std::vector<RowData*>* rightRecords)
{
    // 1. left side has no records: no output.
    if (leftRecords == nullptr || leftRecords->empty()) {
        return;
    }
    // 2. right side has no records: pad null right columns for all left rows.
    if (rightRecords == nullptr || rightRecords->empty()) {
        auto outputBatch = omnistream::VectorBatch::CreateVectorBatch(leftRecords->size(), this->outputTypes);
        this->buildRightNull(leftRecords, outputBatch);
        AbstractStreamOperator<KeyType>::output->collect(outputBatch);
        return;
    }
    // Non-equi per-left emission (matches Flink: a left record's matched pairs are emitted
    // together, followed by its null-padded row when it matches nothing).
    auto emitNonEqui = [this](std::vector<RowData*>* lefts, std::vector<RowData*>* rights) {
        std::vector<RowData*> matchedRight;
        std::vector<RowData*> matchedLeft;
        std::vector<RowData*> singleLeft(1, nullptr);
        for (auto leftRecord : *lefts) {
            matchedRight.clear();
            for (auto rightRecord : *rights) {
                if (this->filter(static_cast<RowData*>(leftRecord), static_cast<RowData*>(rightRecord))) {
                    matchedRight.push_back(rightRecord);
                }
            }
            if (!matchedRight.empty()) {
                // buildInner's non-equi path fills output rows sequentially from parallel
                // left/right arrays (row i = left[i] + right[i]), so repeat the left record
                // once per matched right record.
                matchedLeft.assign(matchedRight.size(), leftRecord);
                auto outputBatch = omnistream::VectorBatch::CreateVectorBatch(matchedRight.size(), this->outputTypes);
                this->buildInner(&matchedLeft, &matchedRight, outputBatch);
                AbstractStreamOperator<KeyType>::output->collect(outputBatch);
            } else {
                singleLeft[0] = leftRecord;
                auto outputBatch = omnistream::VectorBatch::CreateVectorBatch(1, this->outputTypes);
                this->buildRightNull(&singleLeft, outputBatch);
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
        //     so the cartesian product is the set of matched pairs (left-major order, matching
        //     Flink's LeftOuterJoinOperator which iterates left records in the outer loop).
        if (!this->isNonEquiCondition) {
            auto outputBatch =
                omnistream::VectorBatch::CreateVectorBatch(leftRecords->size() * rightRecords->size(), this->outputTypes);
            this->buildInner(leftRecords, rightRecords, outputBatch);
            AbstractStreamOperator<KeyType>::output->collect(outputBatch);
            return;
        }
        // 3b. non-equi condition: per-left emission.
        emitNonEqui(leftRecords, rightRecords);
        return;
    }
    // 4. Mixed / null-key group: split the group into the null-key subgroup (rows can never
    //    match) and the non-null subgroup (standard left-outer semantics). For scalar keys any
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
    // 4a. null-key left rows can never match any row -> right-null-pad them.
    if (!leftNull.empty()) {
        auto outputBatch = omnistream::VectorBatch::CreateVectorBatch(leftNull.size(), this->outputTypes);
        this->buildRightNull(&leftNull, outputBatch);
        AbstractStreamOperator<KeyType>::output->collect(outputBatch);
    }
    // 4b. non-null subgroup: standard left-outer semantics.
    if (rightOk.empty()) {
        if (!leftOk.empty()) {
            auto outputBatch = omnistream::VectorBatch::CreateVectorBatch(leftOk.size(), this->outputTypes);
            this->buildRightNull(&leftOk, outputBatch);
            AbstractStreamOperator<KeyType>::output->collect(outputBatch);
        }
    } else if (!leftOk.empty()) {
        if (!this->isNonEquiCondition) {
            auto outputBatch =
                omnistream::VectorBatch::CreateVectorBatch(leftOk.size() * rightOk.size(), this->outputTypes);
            this->buildInner(&leftOk, &rightOk, outputBatch);
            AbstractStreamOperator<KeyType>::output->collect(outputBatch);
        } else {
            emitNonEqui(&leftOk, &rightOk);
        }
    }
    // 4c. null-key right rows are never emitted by a left-outer join (they can never match),
    //     matching Flink's behaviour for the null-key group.
}

#endif // OMNISTREAM_LEFTOUTERJOINOPERATOR_H
