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

#ifndef OMNISTREAM_SINKV1WRITERCOMMITTABLESERIALIZER_H
#define OMNISTREAM_SINKV1WRITERCOMMITTABLESERIALIZER_H

#include <vector>
#include "connector/kafka/sink/KafkaCommittable.h"
#include "core/io/SimpleVersionedSerializer.h"

template <typename CmmT>
class SinkV1WriterCommittableSerializer : public SimpleVersionedSerializer<std::vector<CmmT>> {
public:
    explicit SinkV1WriterCommittableSerializer(std::shared_ptr<SimpleVersionedSerializer<CmmT>> serializer)
        : serializer(serializer)
    {
    }
    int getVersion() const override;
    std::vector<uint8_t> serialize(const std::vector<CmmT>& obj) override;
    std::vector<CmmT>* deserialize(int version, std::vector<uint8_t>& serialized) override;

private:
    std::shared_ptr<SimpleVersionedSerializer<CmmT>> serializer;
};

#endif // OMNISTREAM_SINKV1WRITERCOMMITTABLESERIALIZER_H
