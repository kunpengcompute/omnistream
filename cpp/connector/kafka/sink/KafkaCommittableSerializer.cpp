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

#include "KafkaCommittableSerializer.h"
#include "core/memory/DataInputDeserializer.h"
#include "core/memory/DataOutputSerializer.h"
#include "core/include/common.h"

int KafkaCommittableSerializer::getVersion() const
{
    return 1;
}

std::vector<uint8_t> KafkaCommittableSerializer::serialize(const KafkaCommittable& state)
{
    DataOutputSerializer out;
    out.writeShort(state.GetEpoch());
    out.writeLong(state.GetProducerId());
    out.writeUTF(state.GetTransactionalId());
    return std::vector<uint8_t>(out.getData(), out.getData() + out.length());
}

KafkaCommittable* KafkaCommittableSerializer::deserialize(int version, std::vector<uint8_t>& serialized)
{
    DataInputDeserializer deserializer(serialized.data(), serialized.size(), 0);
    short epoch = deserializer.readUnsignedShort();
    long producerId = deserializer.readLong();
    std::string transactionalId = deserializer.readUTF();
    return new KafkaCommittable(producerId, epoch, transactionalId, nullptr);
}
