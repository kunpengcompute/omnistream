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

#include "SinkV1WriterCommittableSerializer.h"
#include "core/memory/DataInputDeserializer.h"
#include "connector/kafka/sink/KafkaCommittable.h"
#include "committables/SinkV1CommittableDeserializer.h"

template <typename CmmT>
int SinkV1WriterCommittableSerializer<CmmT>::getVersion() const
{
    return 1;
}

template <typename CmmT>
std::vector<uint8_t> SinkV1WriterCommittableSerializer<CmmT>::serialize(const std::vector<CmmT>& obj)
{
    throw std::runtime_error("This serializer should only be used to deserialize legacy committable state.");
}

template <typename CmmT>
std::vector<CmmT>* SinkV1WriterCommittableSerializer<CmmT>::deserialize(int version, std::vector<uint8_t>& serialized)
{
    DataInputDeserializer deserializer;
    deserializer.setBuffer(serialized.data(), serialized.size(), 0, serialized.size());
    return SinkV1CommittableDeserializer::readVersionAndDeserializeList(serializer, deserializer);
}

template class SinkV1WriterCommittableSerializer<KafkaCommittable>;
