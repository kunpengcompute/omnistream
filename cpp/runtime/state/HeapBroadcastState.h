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

#ifndef OMNISTREAM_HEAPBROADCASTSTATE_H
#define OMNISTREAM_HEAPBROADCASTSTATE_H

#include <memory>
#include <map>
#include <vector>
#include <optional>

#include "core/typeutils/MapSerializer.h"

#include "BackendWritableBroadcastState.h"
#include "RegisteredBroadcastStateBackendMetaInfo.h"

template <typename K, typename V>
class HeapBroadcastState : public BackendWritableBroadcastState<K, V> {
public:
    HeapBroadcastState(
        const std::shared_ptr<RegisteredBroadcastStateBackendMetaInfo>& stateMetaInfo,
        const std::shared_ptr<std::map<K, V>>& internalMap)
        : stateMetaInfo_(stateMetaInfo),
          internalMap_(internalMap),
          internalMapCopySerializer_(
              std::make_shared<MapSerializer>(stateMetaInfo->getKeySerializer(), stateMetaInfo->getValueSerializer()))
    {
    }

    HeapBroadcastState(
        const std::shared_ptr<RegisteredBroadcastStateBackendMetaInfo>& stateMetaInfo,
        const std::shared_ptr<MapSerializer>& internalMapCopySerializer)
        : stateMetaInfo_(stateMetaInfo),
          internalMapCopySerializer_(internalMapCopySerializer)
    {
        initInternalMap();
    }

    HeapBroadcastState(const std::shared_ptr<RegisteredBroadcastStateBackendMetaInfo>& stateMetaInfo)
        : HeapBroadcastState(
              stateMetaInfo,
              std::make_shared<MapSerializer>(stateMetaInfo->getKeySerializer(), stateMetaInfo->getValueSerializer()))
    {
    }

    void setStateMetaInfo(const std::shared_ptr<RegisteredBroadcastStateBackendMetaInfo>& stateMetaInfo) override
    {
        internalMapCopySerializer_ =
            std::make_shared<MapSerializer>(stateMetaInfo->getKeySerializer(), stateMetaInfo->getValueSerializer());
        stateMetaInfo_ = stateMetaInfo;
    }

    std::shared_ptr<RegisteredBroadcastStateBackendMetaInfo> getStateMetaInfo() const override
    {
        return stateMetaInfo_;
    }

    void initInternalMap()
    {
        internalMap_ = std::shared_ptr<std::map<K, V>>();
    }

    void setInternalMap(const std::shared_ptr<std::map<K, V>>& internalMap)
    {
        internalMap_ = internalMap;
    }

    std::shared_ptr<std::map<K, V>> getInternalMap()
    {
        return internalMap_;
    }

    std::shared_ptr<MapSerializer> getInternalMapCopySerializer()
    {
        return internalMapCopySerializer_;
    }

    std::shared_ptr<BackendWritableBroadcastState<K, V>> deepCopy() override
    {
        return std::dynamic_pointer_cast<BackendWritableBroadcastState<K, V>>(copy());
    }

    std::shared_ptr<HeapBroadcastState<K, V>> copy()
    {
        return std::make_shared<HeapBroadcastState<K, V>>(
            std::make_shared<RegisteredBroadcastStateBackendMetaInfo>(*this->stateMetaInfo_),
            std::make_shared<std::map<K, V>>(*this->internalMap_),
            std::make_shared<MapSerializer>(*this->internalMapCopySerializer_));
    }

    long write(long startPos, DataOutputSerializer& out) override
    {
        long offset = startPos + out.getPosition();

        out.writeInt(internalMap_->size());

        int i = 0;
        for (const auto& entry : *internalMap_) {
            getStateMetaInfo()->getKeySerializer()->serialize(&entry.first, out);
            getStateMetaInfo()->getValueSerializer()->serialize(&entry.second, out);
        }

        return offset;
    }

    void put(const K& key, const V& value) override
    {
        (*internalMap_)[key] = value;
    }

    void putAll(const std::map<K, V>& map) override
    {
        for (const auto& entry : map) {
            (*internalMap_)[entry.first] = entry.second;
        }
    }

    void remove(const K& key) override
    {
        internalMap_->erase(key);
    }

    std::optional<V> get(const K& key) const override
    {
        auto iterator = internalMap_->find(key);
        if (iterator != internalMap_->end()) {
            return iterator->second;
        }
        return std::optional<V>{};
    }

    bool contains(const K& key) const override
    {
        return internalMap_->find(key) != internalMap_->end();
    }

    std::vector<std::pair<K, V>> entries() override
    {
        std::vector<std::pair<K, V>> result;
        result.reserve(internalMap_->size());
        for (auto& entry : *internalMap_) {
            result.emplace_back(entry.first, entry.second);
        }
        return result;
    }

    std::vector<std::pair<K, V>> immutableEntries() const override
    {
        std::vector<std::pair<K, V>> result;
        result.reserve(internalMap_->size());
        for (const auto& entry : *internalMap_) {
            result.emplace_back(entry.first, entry.second);
        }
        return result;
    }

    void clear() override
    {
        internalMap_->clear();
    }

private:
    std::shared_ptr<RegisteredBroadcastStateBackendMetaInfo> stateMetaInfo_;
    std::shared_ptr<std::map<K, V>> internalMap_;
    std::shared_ptr<MapSerializer> internalMapCopySerializer_;
};

#endif // OMNISTREAM_HEAPBROADCASTSTATE_H
