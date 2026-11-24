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

#ifndef OMNISTREAM_COMMITTABLECOLLECTORSERIALIZER_H
#define OMNISTREAM_COMMITTABLECOLLECTORSERIALIZER_H

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

#include "CommittableCollector.h"
#include "CheckpointSimpleVersionedSerializer.h"

template <typename CommT>
class CommittableCollectorSerializer : public SimpleVersionedSerializer<CommittableCollector<CommT>> {
public:
    CommittableCollectorSerializer(
        std::shared_ptr<SimpleVersionedSerializer<CommT>> committableSerializer, int subtaskId, int numberOfSubtasks)
        : committableSerializer_(std::move(committableSerializer)),
          subtaskId_(subtaskId),
          numberOfSubtasks_(numberOfSubtasks)
    {
    }

    int getVersion() const override
    {
        return 2;
    }

    std::vector<uint8_t> serialize(const CommittableCollector<CommT>& committableCollector) override
    {
        DataOutputSerializer out(256);
        out.writeInt(MAGIC_NUMBER);
        serializeV2(committableCollector, out);
        return std::vector<uint8_t>(out.getData(), out.getData() + out.length());
    }

    CommittableCollector<CommT>* deserialize(int version, std::vector<uint8_t>& serialized) override
    {
        DataInputDeserializer input(serialized.data(), serialized.size(), 0);
        if (version == 1) {
            return deserializeV1(input);
        }
        if (version == 2) {
            return deserializeV2(input);
        }

        throw std::invalid_argument("Invalid version : " + std::to_string(version));
    }

    void serializeV2(const CommittableCollector<CommT>& committableCollector, DataOutputSerializer& out)
    {
        auto chkComs = committableCollector.getChkCom();

        std::vector<CheckpointCommittableManagerImpl<CommT>> list;
        list.reserve(chkComs.size());
        for (const auto& item : chkComs) {
            list.push_back(item->Copy());
        }

        CheckpointSimpleVersionedSerializer<CommT> checkpointSerializer(
            committableSerializer_, subtaskId_, numberOfSubtasks_);

        SimpleVersionedSerialization::writeVersionAndSerializeList(checkpointSerializer, list, out);
    }

    CommittableCollector<CommT>* deserializeV1(DataInputDeserializer& input)
    {
        validateMagicNumber(input);
        auto* list = SimpleVersionedSerialization::readVersionAndDeserializeList<CommT>(*committableSerializer_, input);
        return new CommittableCollector<CommT>(CommittableCollector<CommT>::ofLegacy(*list));
    }

    CommittableCollector<CommT>* deserializeV2(DataInputDeserializer& input)
    {
        validateMagicNumber(input);

        CheckpointSimpleVersionedSerializer<CommT> checkpointSerializer(
            committableSerializer_, subtaskId_, numberOfSubtasks_);

        std::unique_ptr<std::vector<CheckpointCommittableManagerImpl<CommT>>> list(
            SimpleVersionedSerialization::readVersionAndDeserializeList(checkpointSerializer, input));

        typename CommittableCollector<CommT>::CheckpointCommittableMap checkpointCommittables;
        for (auto& item : *list) {
            long checkpointId = item.GetCheckpointId();
            checkpointCommittables.emplace(
                checkpointId, std::make_shared<CheckpointCommittableManagerImpl<CommT>>(std::move(item)));
        }

        return new CommittableCollector<CommT>(checkpointCommittables, subtaskId_, numberOfSubtasks_);
    }

private:
    static const int MAGIC_NUMBER = 0xb91f252c;

    std::shared_ptr<SimpleVersionedSerializer<CommT>> committableSerializer_;
    int subtaskId_;
    int numberOfSubtasks_;

    static void validateMagicNumber(DataInputDeserializer& input)
    {
        int magicNumber = input.readInt();
        if (magicNumber != MAGIC_NUMBER) {
            throw std::invalid_argument("Invalid magic number : " + std::to_string(magicNumber));
        }
    }
};

#endif // OMNISTREAM_COMMITTABLECOLLECTORSERIALIZER_H
