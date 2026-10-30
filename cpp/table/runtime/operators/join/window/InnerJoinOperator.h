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
    // 3. null join key (Flink JoinConditionWithNullFilters semantics): no pair can match.
    if (this->keyHasFilteredNull(leftRecords)) {
        auto outputBatch = omnistream::VectorBatch::CreateVectorBatch(leftRecords->size(), this->outputTypes);
        this->buildRightNull(leftRecords, outputBatch);
        AbstractStreamOperator<KeyType>::output->collect(outputBatch);
        return;
    }
    // 4. equi condition: rows in the same key group and same window share equal join keys,
    //    so the cartesian product is the set of matched pairs.
    if (!this->isNonEquiCondition) {
        auto outputBatch =
            omnistream::VectorBatch::CreateVectorBatch(leftRecords->size() * rightRecords->size(), this->outputTypes);
        this->buildInner(leftRecords, rightRecords, outputBatch);
        AbstractStreamOperator<KeyType>::output->collect(outputBatch);
        return;
    }
    // 5. non-equi condition: match each left row against right rows via the JIT filter;
    //    matched pairs are emitted together, unmatched left rows are null-padded.
    //    To stay consistent with Flink, rows are emitted per left record: all of its matched
    //    pairs first (one batch), then the null-padded row if it matched nothing.
    std::vector<RowData*> matchedRight;
    std::vector<RowData*> matchedLeft;
    std::vector<RowData*> singleLeft(1, nullptr);
    for (auto leftRecord : *leftRecords) {
        matchedRight.clear();
        for (auto rightRecord : *rightRecords) {
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
    if (leftRecords != nullptr && rightRecords != nullptr) {
        auto outputBatch =
            omnistream::VectorBatch::CreateVectorBatch(leftRecords->size() * rightRecords->size(), this->outputTypes);
        this->buildInner(leftRecords, rightRecords, outputBatch);
        AbstractStreamOperator<KeyType>::output->collect(outputBatch);
    }

    if (rightRecords != nullptr && leftRecords == nullptr) {
        auto outputBatch = omnistream::VectorBatch::CreateVectorBatch(rightRecords->size(), this->outputTypes);
        this->buildLeftNull(rightRecords, outputBatch);
        AbstractStreamOperator<KeyType>::output->collect(outputBatch);
    }
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
