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
#include <rocksdb/db.h>
#include <set>

#include "RocksDBCachingPriorityQueueSet.h"
#include "state/CompositeKeySerializationUtils.h"
#include "state/InternalPriorityQueue.h"
#include "core/typeutils/TypeSerializer.h"
#include "runtime/state/RocksDBWriteBatchWrapper.h"
#include "core/memory/DataInputDeserializer.h"
#include "runtime/state/RocksIteratorWrapper.h"
#include "runtime/state/heap/HeapPriorityQueueElement.h"
#include "core/utils/type_traits_ext.h"

template <typename K, typename T, typename Comparator>
class RocksDBCachingPriorityQueueSet : public InternalPriorityQueue<T>, public HeapPriorityQueueElement {
    static_assert(is_shared_ptr_v<T>, "T should be shared_ptr");

    class RocksBytesIterator {
    public:
        RocksBytesIterator(std::unique_ptr<RocksIteratorWrapper> iterator, RocksDBCachingPriorityQueueSet* self)
            : iterator_(std::move(iterator)),
              self_(self)
        {
            try {
                // todo: Can cache be used to improve performance?
                // rocksDB will seek to the first key >= target, appending a zero byte to make sure the result > target
                std::vector<uint8_t> seekKey;
                seekKey.reserve(self_->seekHint_->size() + 1);
                seekKey.insert(seekKey.end(), self_->seekHint_->begin(), self_->seekHint_->end());
                seekKey.push_back(0);
                iterator_->seek(seekKey);
                currentElement_ = nextElementIfAvailable();
            } catch (const std::exception& e) {
                throw std::runtime_error("Could not initialize ordered iterator, reason: " + std::string(e.what()));
            }
        }

        ~RocksBytesIterator()
        {
            delete currentElement_;
        }

        bool hasNext()
        {
            return currentElement_ != nullptr;
        }

        std::vector<uint8_t>* next()
        {
            if (!hasNext()) {
                THROW_RUNTIME_ERROR("Iterator has no more elements!");
            }

            auto returnElement = currentElement_;
            iterator_->next();
            currentElement_ = nextElementIfAvailable();
            return returnElement;
        }

    private:
        std::vector<uint8_t>* nextElementIfAvailable()
        {
            if (iterator_->isValid()) {
                auto iterKey = iterator_->keyView();
                auto* elementBytes = new std::vector<uint8_t>(iterKey.begin(), iterKey.end());
                if (isPrefixWith(*elementBytes, *self_->groupPrefixBytes_)) {
                    return elementBytes;
                }
                delete elementBytes;
            }
            return nullptr;
        }

        RocksDBCachingPriorityQueueSet* self_;
        std::unique_ptr<RocksIteratorWrapper> iterator_;
        std::vector<uint8_t>* currentElement_;
    };

    class DeserializingIteratorWrapper : public omnistream::utils::Iterator<T> {
    public:
        explicit DeserializingIteratorWrapper(
            std::unique_ptr<RocksBytesIterator> iterator, RocksDBCachingPriorityQueueSet* self)
            : iterator_(std::move(iterator)),
              self_(self)
        {
        }

        bool hasNext() override
        {
            return iterator_->hasNext();
        }

        T next() override
        {
            auto* elementBytes = iterator_->next();
            const auto& element = self_->deserializeElement(*elementBytes);
            delete elementBytes;
            return element;
        }

    private:
        std::unique_ptr<RocksBytesIterator> iterator_;
        RocksDBCachingPriorityQueueSet* self_;
    };

    class TreeOrderedSetCache {
    public:
        struct LexicographicByteComparator {
            // LexicographicByteComparator{}(lhs, rhs) return true if lhs has higher priority than rhs
            bool operator()(const std::vector<uint8_t>* lhs, const std::vector<uint8_t>* rhs) const
            {
                const size_t min_len = std::min(lhs->size(), rhs->size());
                for (size_t i = 0; i < min_len; ++i) {
                    if ((*lhs)[i] != (*rhs)[i]) {
                        return (*lhs)[i] < (*rhs)[i];
                    }
                }
                return lhs->size() < rhs->size();
            }
        };

        explicit TreeOrderedSetCache(int32_t maxSize) : maxSize_(maxSize)
        {
        }

        ~TreeOrderedSetCache()
        {
            for (auto* elementBytes : treeSet_) {
                delete elementBytes;
            }
        }

        int32_t size() const
        {
            return treeSet_.size();
        }

        int32_t max_size() const
        {
            return maxSize_;
        }

        bool isEmpty() const
        {
            return treeSet_.empty();
        }

        bool isFull() const
        {
            return treeSet_.size() >= maxSize_;
        }

        bool add(std::vector<uint8_t>* toAdd)
        {
            return treeSet_.insert(toAdd).second;
        }

        bool remove(std::vector<uint8_t>* toRemove)
        {
            return treeSet_.erase(toRemove) > 0;
        }

        std::vector<uint8_t>* peekFirst()
        {
            if (isEmpty()) {
                return nullptr;
            }
            return *treeSet_.begin();
        }

        std::vector<uint8_t>* peekLast()
        {
            if (isEmpty()) {
                return nullptr;
            }
            return *std::prev(treeSet_.end());
        }

        std::vector<uint8_t>* pollFirst()
        {
            if (isEmpty()) {
                return nullptr;
            }
            auto res = treeSet_.extract(treeSet_.begin());
            return res.value();
        }

        std::vector<uint8_t>* pollLast()
        {
            if (isEmpty()) {
                return nullptr;
            }
            auto res = treeSet_.extract(std::prev(treeSet_.end()));
            return res.value();
        }

        void bulkLoadFromOrderedIterator(RocksBytesIterator* orderedIterator)
        {
            for (auto* elementBytes : treeSet_) {
                delete elementBytes;
            }
            treeSet_.clear();
            auto i = maxSize_;
            while (--i >= 0 && orderedIterator->hasNext()) {
                treeSet_.insert(orderedIterator->next());
            }
        }

    private:
        int32_t maxSize_;
        std::set<std::vector<uint8_t>*, LexicographicByteComparator> treeSet_;
    };

public:
    RocksDBCachingPriorityQueueSet(
        int keyGroupId,
        int keyGroupPrefixBytes,
        rocksdb::DB* db,
        std::shared_ptr<rocksdb::ReadOptions> readOptions,
        rocksdb::ColumnFamilyHandle* columnFamilyHandle,
        TypeSerializer* byteOrderProducingSerializer,
        std::shared_ptr<DataOutputSerializer> outputStream,
        std::shared_ptr<DataInputDeserializer> inputStream,
        std::shared_ptr<RocksDBWriteBatchWrapper> batchWrapper,
        int32_t cacheSize)
        : db_(db),
          readOptions_(std::move(readOptions)),
          columnFamilyHandle_(columnFamilyHandle),
          byteOrderProducingSerializer_(byteOrderProducingSerializer),
          batchWrapper_(batchWrapper),
          outputView_(outputStream),
          inputView_(inputStream),
          orderedCache_(cacheSize),
          allElementsInCache_(false)
    {
        groupPrefixBytes_ = createKeyGroupBytes(keyGroupId, keyGroupPrefixBytes);
        seekHint_ = groupPrefixBytes_.get();
    }

    ~RocksDBCachingPriorityQueueSet() override
    {
        if (seekHint_ != groupPrefixBytes_.get()) {
            delete seekHint_;
        }
    }

    T peek() override
    {
        checkRefillCacheFromStore();

        if (peekCache_ != nullptr) {
            return peekCache_;
        }

        auto firstBytes = orderedCache_.peekFirst();
        if (firstBytes != nullptr) {
            peekCache_ = deserializeElement(*firstBytes);
            return peekCache_;
        }
        return nullptr;
    }

    T poll() override
    {
        checkRefillCacheFromStore();

        auto firstBytes = orderedCache_.pollFirst();
        if (firstBytes == nullptr) {
            return nullptr;
        }
        removeFromRocksDB(*firstBytes);

        if (orderedCache_.isEmpty()) {
            if (seekHint_ != groupPrefixBytes_.get()) {
                delete seekHint_;
            }
            seekHint_ = firstBytes;
        }

        if (peekCache_ != nullptr) {
            auto fromCache = peekCache_;
            peekCache_ = nullptr;
            if (firstBytes != seekHint_) {
                delete firstBytes;
            }
            return fromCache;
        }

        auto res = deserializeElement(*firstBytes);
        if (firstBytes != seekHint_) {
            delete firstBytes;
        }
        return res;
    }

    bool add(const T& toAdd) override
    {
        checkRefillCacheFromStore();

        auto toAddBytes = serializeElement(toAdd);
        bool cacheFull = orderedCache_.isFull();
        auto last = orderedCache_.peekLast();
        bool shouldCache = (!cacheFull && allElementsInCache_) || (last != nullptr && comparator_(toAddBytes, last));

        if (shouldCache) {
            if (cacheFull) {
                delete orderedCache_.pollLast();
                allElementsInCache_ = false;
            }

            if (orderedCache_.add(toAddBytes)) {
                addToRocksDB(*toAddBytes);
                auto first = orderedCache_.peekFirst();
                if (first == toAddBytes) {
                    peekCache_ = nullptr;
                    return true;
                }
            } else {
                delete toAddBytes;
            }
        } else {
            addToRocksDB(*toAddBytes);
            allElementsInCache_ = false;
            delete toAddBytes;
        }
        return false;
    }

    void addAll(const std::vector<T>& elements) override
    {
        if (elements.empty()) {
            return;
        }
        for (const auto& element : elements) {
            add(element);
        }
    }

    bool remove(const T& toRemove) override
    {
        checkRefillCacheFromStore();

        auto oldHead = orderedCache_.peekFirst();
        if (oldHead == nullptr) {
            return false;
        }

        auto toRemoveBytes = serializeElement(toRemove);
        removeFromRocksDB(*toRemoveBytes);
        orderedCache_.remove(toRemoveBytes);

        if (orderedCache_.isEmpty()) {
            if (seekHint_ != groupPrefixBytes_.get()) {
                delete seekHint_;
            }
            seekHint_ = toRemoveBytes;
            peekCache_ = nullptr;
            return true;
        }

        delete toRemoveBytes;

        auto newHead = orderedCache_.peekFirst();
        if (oldHead != newHead) {
            peekCache_ = nullptr;
            return true;
        }
        return false;
    }

    bool isEmpty() override
    {
        checkRefillCacheFromStore();
        return orderedCache_.isEmpty();
    }

    int32_t size() override
    {
        if (allElementsInCache_) {
            return orderedCache_.size();
        }

        int count = 0;
        auto iter = orderedBytesIterator();
        while (iter->hasNext()) {
            delete iter->next();
            ++count;
        }
        return count;
    }

    std::unique_ptr<omnistream::utils::Iterator<T>> iterator() override
    {
        return std::make_unique<DeserializingIteratorWrapper>(std::move(orderedBytesIterator()), this);
    }

private:
    std::unique_ptr<RocksBytesIterator> orderedBytesIterator()
    {
        flushWriteBatch();

        std::unique_ptr<rocksdb::Iterator> rocksIterator;
        rocksIterator.reset(db_->NewIterator(*readOptions_, columnFamilyHandle_));
        auto rocksIteratorWrapper = std::make_unique<RocksIteratorWrapper>(std::move(rocksIterator));

        return std::make_unique<RocksBytesIterator>(std::move(rocksIteratorWrapper), this);
    }

    void flushWriteBatch()
    {
        try {
            batchWrapper_->Flush();
        } catch (const std::exception& e) {
            THROW_RUNTIME_ERROR("Failed to flush write batch: " + std::string(e.what()));
        }
    }

    void addToRocksDB(std::vector<uint8_t>& toAddBytes)
    {
        try {
            rocksdb::Slice slice(reinterpret_cast<const char*>(toAddBytes.data()), toAddBytes.size());
            batchWrapper_->Put(columnFamilyHandle_, slice, rocksdb::Slice());
        } catch (const std::exception& e) {
            THROW_RUNTIME_ERROR("Failed to add data to RocksDB: " + std::string(e.what()));
        }
    }

    void removeFromRocksDB(std::vector<uint8_t>& toRemoveBytes)
    {
        try {
            rocksdb::Slice slice(reinterpret_cast<const char*>(toRemoveBytes.data()), toRemoveBytes.size());
            batchWrapper_->Delete(columnFamilyHandle_, slice);
        } catch (const std::exception& e) {
            THROW_RUNTIME_ERROR("Failed to remove data from RocksDB: " + std::string(e.what()));
        }
    }

    void checkRefillCacheFromStore()
    {
        if (!allElementsInCache_ && orderedCache_.isEmpty()) {
            try {
                auto iter = orderedBytesIterator();
                orderedCache_.bulkLoadFromOrderedIterator(iter.get());
                allElementsInCache_ = !iter->hasNext();
            } catch (const std::exception& e) {
                THROW_RUNTIME_ERROR("Exception while refilling store from iterator: " + std::string(e.what()));
            }
        }
    }

    static bool isPrefixWith(const std::vector<uint8_t>& bytes, const std::vector<uint8_t>& prefix)
    {
        if (bytes.size() < prefix.size()) {
            return false;
        }
        return std::memcmp(bytes.data(), prefix.data(), prefix.size()) == 0;
    }

    std::unique_ptr<std::vector<uint8_t>> createKeyGroupBytes(int32_t keyGroupId, int32_t numPrefixBytes)
    {
        outputView_->clear();
        try {
            CompositeKeySerializationUtils::writeKeyGroup(keyGroupId, numPrefixBytes, *outputView_);
        } catch (const std::exception& e) {
            THROW_RUNTIME_ERROR("Failed to write key group bytes: " + std::string(e.what()));
        }
        return std::unique_ptr<std::vector<uint8_t>>(outputView_->getCopyOfBuffer());
    }

    std::vector<uint8_t>* serializeElement(const T& element)
    {
        try {
            outputView_->clear();
            outputView_->write(*groupPrefixBytes_);
            byteOrderProducingSerializer_->serialize(element.get(), *outputView_);
            return outputView_->getCopyOfBuffer();
        } catch (const std::exception& e) {
            THROW_RUNTIME_ERROR("Failed to serialize element: " + std::string(e.what()));
        }
    }

    T deserializeElement(const std::vector<uint8_t>& bytes)
    {
        try {
            auto numPrefixBytes = groupPrefixBytes_->size();
            inputView_->setBuffer(
                bytes.data(),
                static_cast<int>(bytes.size()),
                static_cast<int>(numPrefixBytes),
                static_cast<int>(bytes.size() - numPrefixBytes));
            using ElementType = typename T::element_type;
            return std::shared_ptr<ElementType>(
                static_cast<ElementType*>(byteOrderProducingSerializer_->deserialize(*inputView_)));
        } catch (const std::exception& e) {
            THROW_RUNTIME_ERROR("Failed to deserialize element: " + std::string(e.what()));
        }
    }

    rocksdb::DB* db_;
    std::shared_ptr<rocksdb::ReadOptions> readOptions_;
    rocksdb::ColumnFamilyHandle* columnFamilyHandle_;
    TypeSerializer* byteOrderProducingSerializer_;
    std::shared_ptr<RocksDBWriteBatchWrapper> batchWrapper_;
    std::unique_ptr<std::vector<uint8_t>> groupPrefixBytes_;
    std::shared_ptr<DataOutputSerializer> outputView_;
    std::shared_ptr<DataInputDeserializer> inputView_;
    TreeOrderedSetCache orderedCache_;
    std::vector<uint8_t>* seekHint_;
    T peekCache_ = nullptr;
    bool allElementsInCache_;
    typename TreeOrderedSetCache::LexicographicByteComparator comparator_;
};
