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

#ifndef OMNISTREAM_CHECKPOINTSIMPLEVERSIONEDSERIALIZER_H
#define OMNISTREAM_CHECKPOINTSIMPLEVERSIONEDSERIALIZER_H

#include <vector>
#include <string>
#include <iostream>
#include <sstream>
#include <cstdint>
#include <memory>

#include "core/include/common.h"
#include "core/memory/DataOutputSerializer.h"
#include "core/memory/DataInputDeserializer.h"
#include "core/io/SimpleVersionedSerializer.h"
#include "core/io/SimpleVersionedSerialization.h"

#include "CheckpointCommittableManagerImpl.h"
#include "SubTaskSimpleVersionedSerializer.h"

template <typename CommT>
class CheckpointSimpleVersionedSerializer : public SimpleVersionedSerializer<CheckpointCommittableManagerImpl<CommT>> {
public:
    CheckpointSimpleVersionedSerializer(
        std::shared_ptr<SimpleVersionedSerializer<CommT>> committableSerializer, int subtaskId, int numberOfSubtasks)
        : committableSerializer_(std::move(committableSerializer)),
          subtaskId_(subtaskId),
          numberOfSubtasks_(numberOfSubtasks)
    {
    }

    int getVersion() const override
    {
        return 0;
    }

    std::vector<uint8_t> serialize(const CheckpointCommittableManagerImpl<CommT>& checkpoint) override
    {
        DataOutputSerializer out(256);
        out.writeLong(checkpoint.GetCheckpointId());

        auto subCMs = checkpoint.getSubCM();

        std::vector<SubtaskCommittableManager<CommT>> list;
        list.reserve(subCMs.size());
        for (const auto& item : subCMs) {
            list.push_back(item->Copy());
        }

        SubTaskSimpleVersionedSerializer<CommT> subTaskSerializer(
            committableSerializer_, subtaskId_, numberOfSubtasks_, checkpoint.GetCheckpointId());

        SimpleVersionedSerialization::writeVersionAndSerializeList(subTaskSerializer, list, out);

        return std::vector<uint8_t>(out.getData(), out.getData() + out.length());
    }

    CheckpointCommittableManagerImpl<CommT>* deserialize(int version, std::vector<uint8_t>& serialized) override
    {
        DataInputDeserializer input(serialized.data(), serialized.size(), 0);

        long checkpointId = input.readLong();

        SubTaskSimpleVersionedSerializer<CommT> subTaskSerializer(
            committableSerializer_, subtaskId_, numberOfSubtasks_, checkpointId);

        std::unique_ptr<std::vector<SubtaskCommittableManager<CommT>>> subCMs(
            SimpleVersionedSerialization::readVersionAndDeserializeList(subTaskSerializer, input));

        typename CheckpointCommittableManagerImpl<CommT>::SubtaskCommittableManagers managers;

        for (auto& item : *subCMs) {
            int subtaskId = item.GetSubtaskId();
            auto it = managers.find(subtaskId);
            if (it != managers.end()) {
                it->second->Merge(item);
            } else {
                managers.emplace(subtaskId, std::make_shared<SubtaskCommittableManager<CommT>>(std::move(item)));
            }
        }

        return new CheckpointCommittableManagerImpl<CommT>(managers, subtaskId_, numberOfSubtasks_, checkpointId);
    }

private:
    std::shared_ptr<SimpleVersionedSerializer<CommT>> committableSerializer_;
    int subtaskId_;
    int numberOfSubtasks_;
};

#endif // OMNISTREAM_CHECKPOINTSIMPLEVERSIONEDSERIALIZER_H
