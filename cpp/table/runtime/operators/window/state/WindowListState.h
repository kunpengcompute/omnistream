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

#include <array>
#include <tuple>
#include "runtime/state/heap/HeapListState.h"
#include "WindowState.h"
/**
 * KeyType: The type of the values that can be added to the list state. such as RowData*
 * W: The data type that the serializer serializes.
 * ValType: The type of the values that can be added to the list state. such as RowData*
 * */
template <typename KeyType, typename W, typename ValType>
class WindowListState : public WindowState<W> {
public:
    WindowListState(InternalListState<KeyType, W, ValType>* windowState) : windowState(windowState) {};
    ~WindowListState() override;
    void clear(W window);
    std::vector<ValType>* get(W window);
    void add(W window, ValType value);

    uint32_t getNextSequenceNumber(int32_t keyGroup);
    void addVectorBatch(int32_t keyGroup, omnistream::VectorBatch* batch);
    void addVectorBatches(const std::unordered_map<int32_t, omnistream::VectorBatch*>& vectorBatchByKeyGroup);
    omnistream::VectorBatch* getVectorBatch(int32_t keyGroup, uint32_t sequenceNumber);
    std::vector<omnistream::VectorBatch*> getVectorBatches(int32_t keyGroup);
    void clearVectorBatches(int64_t currentTimestamp)
    {
        windowState->clearVectorBatches(currentTimestamp);
    }
    void clearVectorBatches(int32_t keyGroup, std::vector<uint32_t>& sequenceNumbersToDelete)
    {
        windowState->clearVectorBatches(keyGroup, sequenceNumbersToDelete);
    }

private:
    InternalListState<KeyType, W, ValType>* windowState;
};

template <typename KeyType, typename W, typename ValType>
void WindowListState<KeyType, W, ValType>::clear(W window)
{
    windowState->setCurrentNamespace(window);
    windowState->clear();
}

template <typename KeyType, typename W, typename ValType>
WindowListState<KeyType, W, ValType>::~WindowListState()
{
    delete windowState;
}

template <typename KeyType, typename W, typename ValType>
std::vector<ValType>* WindowListState<KeyType, W, ValType>::get(W window)
{
    windowState->setCurrentNamespace(window);
    return windowState->get();
}

template <typename KeyType, typename W, typename ValType>
void WindowListState<KeyType, W, ValType>::add(W window, ValType value)
{
    windowState->setCurrentNamespace(window);
    windowState->add(value);
}

template <typename KeyType, typename W, typename ValType>
void WindowListState<KeyType, W, ValType>::addVectorBatch(int32_t keyGroup, omnistream::VectorBatch* batch)
{
    windowState->addVectorBatch(keyGroup, batch);
}

template <typename KeyType, typename W, typename ValType>
void WindowListState<KeyType, W, ValType>::addVectorBatches(
    const std::unordered_map<int32_t, omnistream::VectorBatch*>& vectorBatchByKeyGroup)
{
    windowState->addVectorBatches(vectorBatchByKeyGroup);
}

template <typename KeyType, typename W, typename ValType>
std::vector<omnistream::VectorBatch*> WindowListState<KeyType, W, ValType>::getVectorBatches(int32_t keyGroup)
{
    return windowState->getVectorBatches(keyGroup);
}

template <typename KeyType, typename W, typename ValType>
omnistream::VectorBatch* WindowListState<KeyType, W, ValType>::getVectorBatch(int32_t keyGroup, uint32_t sequenceNumber)
{
    return windowState->getVectorBatch(keyGroup, sequenceNumber);
}

template <typename KeyType, typename W, typename ValType>
uint32_t WindowListState<KeyType, W, ValType>::getNextSequenceNumber(int32_t keyGroup)
{
    return windowState->getNextSequenceNumber(keyGroup);
}
