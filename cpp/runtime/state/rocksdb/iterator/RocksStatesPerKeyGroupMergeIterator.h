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

#ifndef OMNISTREAM_ROCKSSTATESPERKEYGROUPMERGEITERATOR_H
#define OMNISTREAM_ROCKSSTATESPERKEYGROUPMERGEITERATOR_H

#include <queue>
#include <vector>
#include <functional>
#include <algorithm>
#include <cstdint>
#include <stdexcept>
#include <memory>
#include "runtime/state/KeyValueStateIterator.h"
#include "core/fs/CloseableRegistry.h"
#include "runtime/state/RocksIteratorWrapper.h"
#include "SingleStateIterator.h"
#include "RocksSingleStateIterator.h"
#include "common.h"

class RocksStatesPerKeyGroupMergeIterator : public KeyValueStateIterator {
public:
    // Comparator that uses pre-decoded keyGroup integers instead of
    // byte-level prefix comparison. This replaces an O(prefixBytes) memcmp
    // with an O(1) integer compare on the merge-heap hot path.
    // keyGroup() is decoded once per iterator position by refreshKeyGroup().
    struct IteratorComparator {
        bool operator()(
            const std::unique_ptr<SingleStateIterator>& a, const std::unique_ptr<SingleStateIterator>& b) const
        {
            int aKeyGroup = a->keyGroup();
            int bKeyGroup = b->keyGroup();
            if (aKeyGroup != bKeyGroup) {
                // 最小堆需要反转比较结果
                return aKeyGroup > bKeyGroup;
            }

            // 然后按kvStateId比较
            return a->getKvStateId() > b->getKvStateId();
        }
    };

    RocksStatesPerKeyGroupMergeIterator(
        std::unique_ptr<CloseableRegistry> closeableRegistry,
        std::vector<std::pair<std::unique_ptr<RocksIteratorWrapper>, int>>& kvStateIterators,
        std::vector<std::unique_ptr<SingleStateIterator>>& heapPriorityQueueIterators,
        int keyGroupPrefixByteCount)
        : closeableRegistry_(std::move(closeableRegistry)),
          keyGroupPrefixByteCount_(keyGroupPrefixByteCount)
    {
        if (keyGroupPrefixByteCount < 1) {
            throw std::invalid_argument("keyGroupPrefixByteCount must be at least 1");
        }

        // 构建迭代器堆
        if (!kvStateIterators.empty() || !heapPriorityQueueIterators.empty()) {
            buildIteratorHeap(kvStateIterators, heapPriorityQueueIterators);
            valid_ = !heap_.empty();
            if (valid_) {
                currentSubIterator_ = std::move(const_cast<std::unique_ptr<SingleStateIterator>&>(heap_.top()));
                heap_.pop();
            }
        } else {
            valid_ = false;
        }

        newKeyGroup_ = true;
        newKVState_ = true;
        if (valid_) {
            refreshCurrentEntry();
        }
    }

    void next() override
    {
        newKeyGroup_ = false;
        newKVState_ = false;

        if (!valid_) return;

        int oldKeyGroup = current_.keyGroup;

        // 推进当前迭代器
        currentSubIterator_->next();

        if (currentSubIterator_->isValid()) {
            // 检查键组是否变化
            if (currentSubIterator_->keyGroup() != oldKeyGroup) {
                SingleStateIterator* oldIteratorPtr = currentSubIterator_.get();
                auto temp = std::move(currentSubIterator_);
                heap_.push(std::move(temp));
                currentSubIterator_ = std::move(const_cast<std::unique_ptr<SingleStateIterator>&>(heap_.top()));
                heap_.pop();
                newKVState_ = (currentSubIterator_.get() != oldIteratorPtr);
                detectNewKeyGroup(oldKeyGroup);
            }
            refreshCurrentEntry();
        } else {
            // 迭代器已耗尽，关闭并移除
            if (currentSubIterator_) {
                currentSubIterator_->close();
            }

            if (heap_.empty()) {
                currentSubIterator_.reset();
                valid_ = false;
                refreshCurrentEntry();
            } else {
                currentSubIterator_ = std::move(const_cast<std::unique_ptr<SingleStateIterator>&>(heap_.top()));
                heap_.pop();
                newKVState_ = true;
                detectNewKeyGroup(oldKeyGroup);
                refreshCurrentEntry();
            }
        }
    }

    int keyGroup() const override
    {
        return current_.keyGroup;
    }

    ByteView key() const override
    {
        return current_.key;
    }

    ByteView value() const override
    {
        return current_.value;
    }

    int kvStateId() const override
    {
        return current_.kvStateId;
    }

    const CurrentEntry& current() const override
    {
        return current_;
    }

    bool isNewKeyValueState() const override
    {
        return newKVState_;
    }

    bool isNewKeyGroup() const override
    {
        return newKeyGroup_;
    }

    bool isValid() const override
    {
        return valid_;
    }

    void close() override
    {
        if (closeableRegistry_) {
            closeableRegistry_->close();
            closeableRegistry_.reset();
        }

        // 清空堆
        while (!heap_.empty()) {
            heap_.pop();
        }

        if (currentSubIterator_) {
            currentSubIterator_.reset();
        }
        valid_ = false;
        current_ = CurrentEntry{};
    }

    ~RocksStatesPerKeyGroupMergeIterator()
    {
        close();
    }

private:
    std::unique_ptr<CloseableRegistry> closeableRegistry_;
    std::priority_queue<
        std::unique_ptr<SingleStateIterator>,
        std::vector<std::unique_ptr<SingleStateIterator>>,
        IteratorComparator>
        heap_;
    std::unique_ptr<SingleStateIterator> currentSubIterator_;
    int keyGroupPrefixByteCount_;
    bool newKeyGroup_ = false;
    bool newKVState_ = false;
    bool valid_ = false;
    CurrentEntry current_;

    void buildIteratorHeap(
        std::vector<std::pair<std::unique_ptr<RocksIteratorWrapper>, int>>& kvStateIterators,
        std::vector<std::unique_ptr<SingleStateIterator>>& heapPriorityQueueIterators)
    {
        // 处理键值状态迭代器
        for (auto& iterPair : kvStateIterators) {
            auto& rocksIter = iterPair.first;
            int kvStateId = iterPair.second;

            rocksIter->seekToFirst();
            if (rocksIter->isValid()) {
                auto wrappingIter = std::make_unique<RocksSingleStateIterator>(
                    std::move(rocksIter), kvStateId, keyGroupPrefixByteCount_);

                heap_.push(std::move(wrappingIter));
            } else {
                rocksIter->close();
            }
        }

        // 处理堆优先队列迭代器
        for (auto& heapIter : heapPriorityQueueIterators) {
            if (heapIter->isValid()) {
                heap_.push(std::move(heapIter));
            } else {
                heapIter->close();
            }
        }
    }

    void detectNewKeyGroup(int oldKeyGroup)
    {
        if (currentSubIterator_->keyGroup() != oldKeyGroup) {
            newKeyGroup_ = true;
        }
    }

    void refreshCurrentEntry()
    {
        if (!valid_ || !currentSubIterator_ || !currentSubIterator_->isValid()) {
            current_ = CurrentEntry{};
            return;
        }
        current_.key = currentSubIterator_->key();
        current_.value = currentSubIterator_->value();
        current_.keyGroup = currentSubIterator_->keyGroup();
        current_.kvStateId = currentSubIterator_->getKvStateId();
        current_.newKeyGroup = newKeyGroup_;
        current_.newKeyValueState = newKVState_;
    }
};
#endif // OMNISTREAM_ROCKSSTATESPERKEYGROUPMERGEITERATOR_H
