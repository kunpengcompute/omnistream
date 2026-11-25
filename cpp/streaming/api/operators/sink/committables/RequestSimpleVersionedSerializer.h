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

#ifndef OMNISTREAM_REQUESTSIMPLEVERSIONEDSERIALIZER_H
#define OMNISTREAM_REQUESTSIMPLEVERSIONEDSERIALIZER_H

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

#include "CommitRequestImpl.h"

template <typename CommT>
class RequestSimpleVersionedSerializer : public SimpleVersionedSerializer<CommitRequestImpl<CommT>> {
public:
    RequestSimpleVersionedSerializer(std::shared_ptr<SimpleVersionedSerializer<CommT>> committableSerializer)
        : committableSerializer_(std::move(committableSerializer))
    {
    }

    int getVersion() const override
    {
        return 0;
    }

    std::vector<uint8_t> serialize(const CommitRequestImpl<CommT>& request) override
    {
        DataOutputSerializer out(256);

        SimpleVersionedSerialization::writeVersionAndSerialize(*committableSerializer_, request.GetCommittable(), out);

        out.writeInt(request.GetNumberOfRetries());
        out.writeInt(static_cast<int>(request.GetState()));

        return std::vector<uint8_t>(out.getData(), out.getData() + out.length());
    }

    CommitRequestImpl<CommT>* deserialize(int version, std::vector<uint8_t>& serialized) override
    {
        DataInputDeserializer input(serialized.data(), serialized.size(), 0);

        std::unique_ptr<CommT> committable(
            SimpleVersionedSerialization::readVersionAndDeSerialize(*committableSerializer_, input));

        int numberOfRetries = input.readInt();
        auto state = static_cast<CommitRequestState>(input.readInt());

        return new CommitRequestImpl<CommT>(*committable, numberOfRetries, state);
    }

private:
    std::shared_ptr<SimpleVersionedSerializer<CommT>> committableSerializer_;
};

#endif // OMNISTREAM_REQUESTSIMPLEVERSIONEDSERIALIZER_H
