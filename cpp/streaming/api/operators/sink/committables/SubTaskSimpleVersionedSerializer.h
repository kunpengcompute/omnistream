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

#ifndef OMNISTREAM_SUBTASKSIMPLEVERSIONEDSERIALIZER_H
#define OMNISTREAM_SUBTASKSIMPLEVERSIONEDSERIALIZER_H

#include <vector>
#include <string>
#include <iostream>
#include <sstream>
#include <cstdint>

#include "core/include/common.h"
#include "core/memory/DataOutputSerializer.h"
#include "core/memory/DataInputDeserializer.h"
#include "core/io/SimpleVersionedSerializer.h"
#include "core/io/SimpleVersionedSerialization.h"

#include "SubtaskCommittableManager.h"
#include "RequestSimpleVersionedSerializer.h"

template <typename CommT>
class SubTaskSimpleVersionedSerializer : public SimpleVersionedSerializer<SubtaskCommittableManager<CommT>> {
public:
    SubTaskSimpleVersionedSerializer(
        std::shared_ptr<SimpleVersionedSerializer<CommT>> committableSerializer,
        int subtaskId,
        int numberOfSubtasks,
        long checkpointId)
        : committableSerializer_(std::move(committableSerializer)),
          subtaskId_(subtaskId),
          numberOfSubtasks_(numberOfSubtasks),
          checkpointId_(checkpointId)
    {
    }

    int getVersion() const override
    {
        return 0;
    }

    std::vector<uint8_t> serialize(const SubtaskCommittableManager<CommT>& subtask) override
    {
        DataOutputSerializer out(256);

        auto requests = subtask.GetRequests();

        std::vector<CommitRequestImpl<CommT>> list;
        list.reserve(requests.size());
        for (const auto& item : requests) {
            list.push_back(*(item->Copy()));
        }

        RequestSimpleVersionedSerializer<CommT> requestSerializer(committableSerializer_);

        SimpleVersionedSerialization::writeVersionAndSerializeList(requestSerializer, list, out);

        out.writeInt(subtask.GetNumCommittables());
        out.writeInt(subtask.GetNumDrained());
        out.writeInt(subtask.GetNumFailed());

        return std::vector<uint8_t>(out.getData(), out.getData() + out.length());
    }

    SubtaskCommittableManager<CommT>* deserialize(int version, std::vector<uint8_t>& serialized) override
    {
        DataInputDeserializer input(serialized.data(), serialized.size(), 0);

        RequestSimpleVersionedSerializer<CommT> requestSerializer(committableSerializer_);

        auto* list = SimpleVersionedSerialization::readVersionAndDeserializeList(requestSerializer, input);

        std::vector<std::shared_ptr<CommitRequestImpl<CommT>>> requests;
        requests.reserve(list->size());
        for (auto& item : *list) {
            requests.push_back(std::make_shared<CommitRequestImpl<CommT>>(std::move(item)));
        }
        delete list;
        return new SubtaskCommittableManager<CommT>(
            requests,
            input.readInt(),
            input.readInt(),
            input.readInt(),
            subtaskId_,
            std::optional<long>(checkpointId_));
    }

private:
    std::shared_ptr<SimpleVersionedSerializer<CommT>> committableSerializer_;
    int subtaskId_;
    int numberOfSubtasks_;
    long checkpointId_;
};

#endif // OMNISTREAM_SUBTASKSIMPLEVERSIONEDSERIALIZER_H
