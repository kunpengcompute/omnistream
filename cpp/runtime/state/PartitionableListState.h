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

#ifndef OMNISTREAM_PARTITIONABLELISTSTATE_H
#define OMNISTREAM_PARTITIONABLELISTSTATE_H

#include <memory>
#include <string>
#include <vector>
#include <set>

#include "core/memory/DataOutputSerializer.h"
#include "core/api/common/state/ListState.h"
#include "core/typeutils/ListSerializer.h"

#include "RegisteredOperatorStateBackendMetaInfo.h"

template <typename S>
class PartitionableListState : public ListState<S> {
public:
    PartitionableListState(
        const std::shared_ptr<RegisteredOperatorStateBackendMetaInfo>& stateMetaInfo,
        const std::shared_ptr<std::vector<S>>& internalList)
        : stateMetaInfo_(stateMetaInfo),
          internalList_(internalList),
          internalListCopySerializer_(std::make_shared<ListSerializer>(stateMetaInfo->getStateSerializer(), false))
    {
    }

    PartitionableListState(
        const std::shared_ptr<RegisteredOperatorStateBackendMetaInfo>& stateMetaInfo,
        const std::shared_ptr<ListSerializer>& internalListCopySerializer)
        : stateMetaInfo_(stateMetaInfo),
          internalListCopySerializer_(internalListCopySerializer)
    {
        initInternalList();
    }

    PartitionableListState(const std::shared_ptr<RegisteredOperatorStateBackendMetaInfo>& stateMetaInfo)
        : PartitionableListState(
              stateMetaInfo, std::make_shared<ListSerializer>(stateMetaInfo->getStateSerializer(), false))
    {
    }

    void setStateMetaInfo(const std::shared_ptr<RegisteredOperatorStateBackendMetaInfo>& stateMetaInfo)
    {
        // The descriptor/meta-info supplies the serializer; this state only borrows it.
        internalListCopySerializer_ = std::make_shared<ListSerializer>(stateMetaInfo->getStateSerializer(), false);
        stateMetaInfo_ = stateMetaInfo;
    }

    std::shared_ptr<RegisteredOperatorStateBackendMetaInfo> getStateMetaInfo()
    {
        return stateMetaInfo_;
    }

    void initInternalList()
    {
        internalList_ = std::make_shared<std::vector<S>>();
    }

    void setInternalList(const std::shared_ptr<std::vector<S>>& internalList)
    {
        internalList_ = internalList;
    }

    std::shared_ptr<std::vector<S>> getInternalList()
    {
        return internalList_;
    }

    std::shared_ptr<ListSerializer> getInternalListCopySerializer()
    {
        return internalListCopySerializer_;
    }

    std::shared_ptr<PartitionableListState<S>> deepCopy()
    {
        return std::make_shared<PartitionableListState<S>>(
            std::make_shared<RegisteredOperatorStateBackendMetaInfo>(*this->stateMetaInfo_),
            // Construct a fresh borrowing wrapper instead of shallow-copying
            // ListSerializer's elementSerializer and TypeSerializer's reuseBuffer.
            std::make_shared<std::vector<S>>(*this->internalList_));
    }

    void add(const S& value) override
    {
        internalList_->push_back(value);
    }

    std::vector<long> write(long startPos, DataOutputSerializer& out)
    {
        std::vector<long> offsets;

        for (size_t i = 0; i < internalList_->size(); i++) {
            auto element = (*internalList_)[i];
            offsets.push_back(startPos + out.getPosition());
            getStateMetaInfo()->getStateSerializer()->serialize(&element, out);
        }

        return offsets;
    }

    void update(const std::vector<S>& values) override
    {
        clear();
        addAll(values);
    }

    std::vector<S>* get() override
    {
        return internalList_.get();
    }

    void merge(const std::vector<S>& other) override
    {
        if (other.empty()) {
            return;
        }
        std::set<S> existSet(internalList_->begin(), internalList_->end());
        for (const S& element : other) {
            if (existSet.find(element) == existSet.end()) {
                existSet.insert(element);
                internalList_->push_back(element);
            }
        }
    }

    void addAll(const std::vector<S>& values) override
    {
        if (values.empty()) {
            return;
        }
        internalList_->reserve(internalList_->size() + values.size());
        internalList_->insert(internalList_->end(), values.begin(), values.end());
    }

    void clear() override
    {
        internalList_->clear();
    }

private:
    std::shared_ptr<RegisteredOperatorStateBackendMetaInfo> stateMetaInfo_;
    std::shared_ptr<std::vector<S>> internalList_;
    std::shared_ptr<ListSerializer> internalListCopySerializer_;
};

#endif // OMNISTREAM_PARTITIONABLELISTSTATE_H
