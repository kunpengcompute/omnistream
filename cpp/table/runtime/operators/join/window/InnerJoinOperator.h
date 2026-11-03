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
