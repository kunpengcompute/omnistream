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

#include <string>
#include "KafkaWriterStateSerializer.h"
#include <sstream>

#include "core/include/common.h"
#include "core/memory/DataOutputSerializer.h"
#include "core/memory/DataInputDeserializer.h"

int KafkaWriterStateSerializer::getVersion() const
{
    return 1;
}

std::vector<uint8_t> KafkaWriterStateSerializer::serialize(const KafkaWriterState& obj)
{
    DataOutputSerializer out;
    out.writeUTF(obj.getTransactionalIdPrefix());
    return std::vector<uint8_t>(out.getData(), out.getData() + out.length());
}

KafkaWriterState* KafkaWriterStateSerializer::deserialize(int version, std::vector<uint8_t>& serialized)
{
    DataInputDeserializer deserializer;
    deserializer.setBuffer(serialized.data(), serialized.size(), 0, serialized.size());
    std::string transactionalIdPrefix = deserializer.readUTF();
    return new KafkaWriterState(transactionalIdPrefix);
}
