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

#ifndef FLINK_TNEL_ABSTRACTSTREAMTASKNETWORKINPUT_H
#define FLINK_TNEL_ABSTRACTSTREAMTASKNETWORKINPUT_H

#include <memory>
#include <unordered_map>
#include <memory>
#include <unordered_map>

#include "streaming/runtime/io/DataInputStatus.h"
#include "streaming/runtime/io/DataOutput.h"
#include "runtime/io/network/api/serialization/RecordDeserializer.h"
#include "runtime/io/network/api/serialization/DeserializationResult.h"
#include "runtime/io/network/api/serialization/SpillingAdaptiveSpanningRecordDeserializer.h"
#include "core/typeutils/TypeSerializer.h"
#include "runtime/plugable/DeserializationDelegate.h"
#include "streaming/runtime/streamrecord/StreamElement.h"
#include "core/include/common.h"

namespace omnistream::datastream {
class AbstractStreamTaskNetworkInput {
public:
    AbstractStreamTaskNetworkInput(
        TypeSerializer* type_serializer,
        std::unique_ptr<std::unordered_map<long, RecordDeserializer*>>&& recordDeserializers);

    ~AbstractStreamTaskNetworkInput();

    uint32_t emitNextProcessElement(DataOutput& output, int32_t& inputNumber);

    virtual void emitNextProcessBuffer(const uint8_t* buffer, size_t length, long channelInfo);

    [[nodiscard]] RecordDeserializer* getActiveSerializer(long channelInfo) const;

    static __attribute__((always_inline)) bool processElement(
        StreamElement* streamElement, DataOutput& output, int32_t& inputNumber)
    {
        if (streamElement->getTag() == StreamElementTag::TAG_REC_WITHOUT_TIMESTAMP ||
            streamElement->getTag() == StreamElementTag::TAG_REC_WITH_TIMESTAMP) {
            output.emitRecord(static_cast<StreamRecord*>(streamElement));
            ++inputNumber;
            return false;
        } else if (streamElement->getTag() == StreamElementTag::TAG_WATERMARK) {
            output.emitWatermark(static_cast<Watermark*>(streamElement));
            return false;
        } else if (streamElement->getTag() == StreamElementTag::TAG_RECORD_ATTRIBUTES) {
            /**
             recordAttributesCombiner.inputRecordAttributes(
                     streamElement.asRecordAttributes(),
                     flattenedChannelIndices.get(lastChannel),
                     outputToOut);
             **/
            return true;
        } else {
            LOG("Bypass the tag for now, tag  is " + std::to_string(static_cast<int>(streamElement->getTag())));
            // throw std::logic_error("Unknown type of StreamElement");
            return false;
        }
    }

protected:
    DeserializationDelegate* deserializationDelegate_;

private:
    RecordDeserializer* currentRecordDeserializer_;

    std::unique_ptr<std::unordered_map<long, RecordDeserializer*>> recordDeserializers_;
    std::unordered_map<long, RecordDeserializer*>* rawRecordDeserializers_;

    long lastChannel_ = -1;
};
} // namespace omnistream::datastream

#endif
