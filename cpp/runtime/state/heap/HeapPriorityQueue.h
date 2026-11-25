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

#include "state/InternalPriorityQueue.h"
#include "utils/Iterator.h"
#include "HeapPriorityQueueElement.h"

/**
 *
 * @tparam T
 * @tparam Comparator Comparator{}(first, second) return true means the first has a higher priority than the second
 * (first will be put before the second)
 */
template <typename T, typename Comparator>
class HeapPriorityQueue : virtual public InternalPriorityQueue<T> {
    static constexpr int32_t HEAD_ELEMENT_INDEX = 1;

    // This limit is same as Flink
    static constexpr int32_t MAX_ARRAY_SIZE = INT32_MAX - 8;

    class HeapIterator : public omnistream::utils::Iterator<T> {
    public:
        HeapIterator(HeapPriorityQueue* self) : self_(self)
        {
            runningIdx_ = HEAD_ELEMENT_INDEX;
            endIdx_ = runningIdx_ + self_->size_;
        }

        bool hasNext() override
        {
            return runningIdx_ < endIdx_;
        }

        T next() override
        {
            if (runningIdx_ >= endIdx_) {
                THROW_RUNTIME_ERROR("Iterator has no next element.");
            }
            return self_->elements_[runningIdx_++];
        }

    private:
        HeapPriorityQueue* self_;
        int32_t runningIdx_;
        int32_t endIdx_;
    };

public:
    HeapPriorityQueue(int32_t minimumCapacity) : minimumCapacity_(minimumCapacity + HEAD_ELEMENT_INDEX)
    {
        if (minimumCapacity_ <= 0 || minimumCapacity_ >= MAX_ARRAY_SIZE) {
            THROW_LOGIC_EXCEPTION("minimumCapacity must be greater than 0 and less than MAX_ARRAY_SIZE.");
        }
        elements_.reserve(minimumCapacity_);
        elements_.resize(HEAD_ELEMENT_INDEX);
    }

    T poll() override
    {
        if (size_ > 0) {
            return removeInternal(HEAD_ELEMENT_INDEX);
        }
        return nullptr;
    }

    T peek() override
    {
        if (size_ > 0) {
            return elements_[HEAD_ELEMENT_INDEX];
        }
        return nullptr;
    }

    bool add(const T& toAdd) override
    {
        addInternal(toAdd);
        return toAdd->getInternalIndex() == HEAD_ELEMENT_INDEX;
    }

    bool remove(const T& toRemove) override
    {
        int32_t removeIdx = toRemove->getInternalIndex();
        if (removeIdx == HeapPriorityQueueElement::NOT_CONTAINED) {
            GErrorLog("HeapPriorityQueue::remove, toRemove is not contained in the queue.");
            return false;
        }
        removeInternal(removeIdx);
        return removeIdx == HEAD_ELEMENT_INDEX;
    }

    bool isEmpty() override
    {
        return size_ == 0;
    }

    int32_t size() override
    {
        return size_;
    }

    void addAll(const std::vector<T>& elements) override
    {
        if (elements.empty()) {
            return;
        }

        reserveForBulkLoad(elements.size());

        for (const auto& element : elements) {
            add(element);
        }
    }

    std::vector<T> toArray()
    {
        auto start = elements_.begin() + HEAD_ELEMENT_INDEX;
        return std::vector<T>(start, start + size_);
    }

    std::unique_ptr<omnistream::utils::Iterator<T>> iterator() override
    {
        return std::make_unique<HeapIterator>(this);
    }

    void adjustModifiedElement(const T& element)
    {
        int32_t idx = element->getInternalIndex();
        if (element == elements_[idx]) {
            adjustElementAtIndex(element, idx);
        }
    }

protected:
    void addInternal(const T& toAdd)
    {
        reserveForAddOne();
        moveElementToIdx(toAdd, ++size_);
        siftUp(size_);
    }

    T removeInternal(int32_t removeIdx)
    {
        auto removedValue = elements_[removeIdx];

        if (removedValue->getInternalIndex() != removeIdx) {
            THROW_RUNTIME_ERROR("Internal index of removed value is not equal to remove index.");
        }
        auto oldSize = size_;

        if (removeIdx != oldSize) {
            auto element = elements_[oldSize];
            moveElementToIdx(element, removeIdx);
            adjustElementAtIndex(element, removeIdx);
        }

        elements_[oldSize] = nullptr;
        --size_;
        // TODO: implement shrinking
        return removedValue;
    }

private:
    void moveElementToIdx(const T& element, int32_t idx)
    {
        elements_[idx] = element;
        element->setInternalIndex(idx);
    }

    void adjustElementAtIndex(const T& element, int32_t idx)
    {
        siftDown(idx);
        if (elements_[idx] == element) {
            siftUp(idx);
        }
    }

    void siftUp(int32_t idx)
    {
        auto currentElement = elements_[idx];
        int32_t parentIdx = idx >> 1;

        while (parentIdx > 0 && isElementPriorityHigherThen_(currentElement, elements_[parentIdx])) {
            moveElementToIdx(elements_[parentIdx], idx);
            idx = parentIdx;
            parentIdx >>= 1;
        }

        moveElementToIdx(currentElement, idx);
    }

    void siftDown(int32_t idx)
    {
        auto currentElement = elements_[idx];
        int32_t firstChildIdx = idx << 1;
        int32_t secondChildIdx = firstChildIdx + 1;

        if (isElementIndexValid(secondChildIdx, size_) &&
            isElementPriorityHigherThen_(elements_[secondChildIdx], elements_[firstChildIdx])) {
            firstChildIdx = secondChildIdx;
        }

        while (isElementIndexValid(firstChildIdx, size_) &&
               isElementPriorityHigherThen_(elements_[firstChildIdx], currentElement)) {
            moveElementToIdx(elements_[firstChildIdx], idx);
            idx = firstChildIdx;
            firstChildIdx = idx << 1;
            secondChildIdx = firstChildIdx + 1;

            if (isElementIndexValid(secondChildIdx, size_) &&
                isElementPriorityHigherThen_(elements_[secondChildIdx], elements_[firstChildIdx])) {
                firstChildIdx = secondChildIdx;
            }
        }

        moveElementToIdx(currentElement, idx);
    }

    bool isElementIndexValid(int32_t idx, int32_t size)
    {
        return idx <= size;
    }

    void reserveForBulkLoad(int32_t toAddSize)
    {
        int32_t minCapacity = size_ + toAddSize + HEAD_ELEMENT_INDEX;
        if (minCapacity > elements_.capacity()) {
            int32_t desiredCapacity = minCapacity + (minCapacity >> 3);
            reserveCapacity(desiredCapacity, minCapacity);
        }
        elements_.resize(minCapacity);
    }

    void reserveCapacity(int32_t desiredCapacity, int32_t minCapacity)
    {
        if (isValidCapacity(desiredCapacity)) {
            elements_.reserve(desiredCapacity);
        } else if (isValidCapacity(minCapacity)) {
            elements_.reserve(MAX_ARRAY_SIZE);
        } else {
            THROW_RUNTIME_ERROR(
                "Required min capacity " << minCapacity << " exceeds maximum capacity " << MAX_ARRAY_SIZE << ".");
        }
    }

    static bool isValidCapacity(int32_t capacity)
    {
        return capacity >= HEAD_ELEMENT_INDEX && capacity <= MAX_ARRAY_SIZE;
    }

    void reserveForAddOne()
    {
        int32_t oldCapacity = elements_.capacity();
        int32_t minCapacity = 1 + size_ + HEAD_ELEMENT_INDEX;
        if (minCapacity > oldCapacity) {
            int32_t grow = oldCapacity < 64 ? oldCapacity + 2 : oldCapacity >> 1;
            reserveCapacity(oldCapacity + grow, minCapacity);
        }
        elements_.resize(minCapacity);
    }

    std::vector<T> elements_;
    int32_t size_ = 0;
    int32_t minimumCapacity_ = 0;
    Comparator isElementPriorityHigherThen_;
};
