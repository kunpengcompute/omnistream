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

#ifndef OMNIFLINK_KAFKACOMMITTABLESERIALIZER_H
#define OMNIFLINK_KAFKACOMMITTABLESERIALIZER_H

#include <vector>
#include <string>
#include <iostream>
#include <sstream>
#include "KafkaCommittable.h"
#include "core/io/SimpleVersionedSerializer.h"

class KafkaCommittableSerializer : public SimpleVersionedSerializer<KafkaCommittable> {
public:
    int getVersion() const override;
    std::vector<uint8_t> serialize(const KafkaCommittable& obj) override;
    KafkaCommittable* deserialize(int version, std::vector<uint8_t>& serialized) override;
};

#endif // OMNIFLINK_KAFKACOMMITTABLESERIALIZER_H
