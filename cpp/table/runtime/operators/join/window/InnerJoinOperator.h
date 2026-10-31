/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2024. All rights reserved.
 */

#ifndef OMNISTREAM_WINDOWOPERATORS_H
#define OMNISTREAM_WINDOWOPERATORS_H
#include "WindowJoinOperator.h"

template <typename KeyType>
class InnerJoinOperator : public WindowJoinOperator<KeyType> {
public:
    InnerJoinOperator(
        const nlohmann::json& config, Output* output, TypeSerializer* leftSerializer, TypeSerializer* rightSerializer);
    void join(std::vector<RowData*>* leftRecords, std::vector<RowData*>* rightRecords) override;
};

template <typename KeyType>
InnerJoinOperator<KeyType>::InnerJoinOperator(
    const nlohmann::json& config, Output* output, TypeSerializer* leftSerializer, TypeSerializer* rightSerializer)
    : WindowJoinOperator<KeyType>(config, output, leftSerializer, rightSerializer)
{
}

template <typename KeyType>
void InnerJoinOperator<KeyType>::join(std::vector<RowData*>* leftRecords, std::vector<RowData*>* rightRecords)
{
    if (leftRecords == nullptr || rightRecords == nullptr) {
        return;
    }
    if (!this->isNonEquiCondition) {
        auto outputBatch =
            omnistream::VectorBatch::CreateVectorBatch(leftRecords->size() * rightRecords->size(), this->outputTypes);
        this->buildInner(leftRecords, rightRecords, outputBatch);
        this->output->collect(outputBatch);
        return;
    }

    std::vector<RowData*> filteredLeft;
    std::vector<RowData*> filteredRight;
    for (auto leftRecord : *leftRecords) {
        for (auto rightRecord : *rightRecords) {
            if (this->filter(static_cast<RowData*>(leftRecord), static_cast<RowData*>(rightRecord))) {
                filteredLeft.push_back(leftRecord);
                filteredRight.push_back(rightRecord);
            }
        }
    }
    if (filteredLeft.size() != 0) {
        auto outputBatch = omnistream::VectorBatch::CreateVectorBatch(filteredLeft.size(), this->outputTypes);
        this->buildInner(&filteredLeft, &filteredRight, outputBatch);
        this->output->collect(outputBatch);
    }
}

template <typename KeyType>
class SemiAntiJoinOperator : public WindowJoinOperator<KeyType> {
public:
    SemiAntiJoinOperator(
        const nlohmann::json& config,
        Output* output,
        TypeSerializer* leftSerializer,
        TypeSerializer* rightSerializer,
        bool isAntiJoin);
    void join(std::vector<RowData*>* leftRecords, std::vector<RowData*>* rightRecords) override;

private:
    bool isAntiJoin;
};

template <typename KeyType>
SemiAntiJoinOperator<KeyType>::SemiAntiJoinOperator(
    const nlohmann::json& config,
    Output* output,
    TypeSerializer* leftSerializer,
    TypeSerializer* rightSerializer,
    bool isAntiJoin)
    : WindowJoinOperator<KeyType>(config, output, leftSerializer, rightSerializer),
      isAntiJoin(isAntiJoin)
{
}

template <typename KeyType>
void SemiAntiJoinOperator<KeyType>::join(std::vector<RowData*>* leftRecords, std::vector<RowData*>* rightRecords)
{
}

template <typename KeyType>
class AbstractOuterJoinOperator : public WindowJoinOperator<KeyType> {
public:
    AbstractOuterJoinOperator(
        const nlohmann::json& config, Output* output, TypeSerializer* leftSerializer, TypeSerializer* rightSerializer);
};

template <typename KeyType>
AbstractOuterJoinOperator<KeyType>::AbstractOuterJoinOperator(
    const nlohmann::json& config, Output* output, TypeSerializer* leftSerializer, TypeSerializer* rightSerializer)
    : WindowJoinOperator<KeyType>(config, output, leftSerializer, rightSerializer)
{
}

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
    // 3. null-free group: all rows share the same (non-null) join key, so the fast paths apply.
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
    // 4. Mixed / null-key group (possible when the key type cannot carry the null flag, e.g.
    //    a scalar KeyType): split the group into the null-key subgroup (rows can never match)
    //    and the non-null subgroup (standard left-outer semantics). This mirrors Flink, where
    //    null-key rows live in their own key group and are filtered by JoinConditionWithNullFilters.
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
    // 3. null-free group: all rows share the same (non-null) join key, so the fast paths apply.
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
    // 4. Mixed / null-key group (possible when the key type cannot carry the null flag, e.g.
    //    a scalar KeyType): split the group into the null-key subgroup (rows can never match)
    //    and the non-null subgroup (standard right-outer semantics). This mirrors Flink, where
    //    null-key rows live in their own key group and are filtered by JoinConditionWithNullFilters.
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

template <typename KeyType>
class FullOuterJoinOperator : public AbstractOuterJoinOperator<KeyType> {
public:
    FullOuterJoinOperator(
        const nlohmann::json& config, Output* output, TypeSerializer* leftSerializer, TypeSerializer* rightSerializer);
    void join(std::vector<RowData*>* leftRecords, std::vector<RowData*>* rightRecords) override;
};

template <typename KeyType>
FullOuterJoinOperator<KeyType>::FullOuterJoinOperator(
    const nlohmann::json& config, Output* output, TypeSerializer* leftSerializer, TypeSerializer* rightSerializer)
    : AbstractOuterJoinOperator<KeyType>(config, output, leftSerializer, rightSerializer)
{
}

template <typename KeyType>
void FullOuterJoinOperator<KeyType>::join(std::vector<RowData*>* leftRecords, std::vector<RowData*>* rightRecords)
{
    if (leftRecords != nullptr && rightRecords != nullptr) {
        auto outputBatch =
            omnistream::VectorBatch::CreateVectorBatch(leftRecords->size() * rightRecords->size(), this->outputTypes);
        this->buildInner(leftRecords, rightRecords, outputBatch);
        AbstractStreamOperator<KeyType>::output->collect(outputBatch);
    }

    if (leftRecords != nullptr && rightRecords == nullptr) {
        auto outputBatch = omnistream::VectorBatch::CreateVectorBatch(leftRecords->size(), this->outputTypes);
        this->buildRightNull(leftRecords, outputBatch);
        AbstractStreamOperator<KeyType>::output->collect(outputBatch);
    }

    if (rightRecords != nullptr && leftRecords == nullptr) {
        auto outputBatch = omnistream::VectorBatch::CreateVectorBatch(rightRecords->size(), this->outputTypes);
        this->buildLeftNull(rightRecords, outputBatch);
        AbstractStreamOperator<KeyType>::output->collect(outputBatch);
    }
}

#endif // OMNISTREAM_WINDOWOPERATORS_H
