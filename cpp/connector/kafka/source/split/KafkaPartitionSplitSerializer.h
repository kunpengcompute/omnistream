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

#ifndef FLINK_TNEL_SIMPLEVERSIONEDSERIALIZER_H
#define FLINK_TNEL_SIMPLEVERSIONEDSERIALIZER_H

#include <vector>
#include <stdexcept>
#include <memory>
#include "KafkaPartitionSplit.h"
#include "io/SimpleVersionedSerializer.h"

class KafkaPartitionSplitSerializer : public SimpleVersionedSerializer<KafkaPartitionSplit> {
public:
    int getVersion() const override;
    std::vector<uint8_t> serialize(const KafkaPartitionSplit& split) override;
    KafkaPartitionSplit* deserialize(int version, std::vector<uint8_t>& serialized) override;

    inline static const int CURRENT_VERSION = 0;
    inline static const int ONE_BYTE_LENGTH = 8;
    inline static const int TWO_BYTE_LENGTH = 16;
    inline static const int THREE_BYTE_LENGTH = 24;
};

#endif // FLINK_TNEL_SIMPLEVERSIONEDSERIALIZER_H
