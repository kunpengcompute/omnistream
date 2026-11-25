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

#ifndef OMNISTREAM_RECORDFILTER_H
#define OMNISTREAM_RECORDFILTER_H

#include <memory>
#include <functional>

#include "runtime/plugable/SerializationDelegate.h"
#include "streaming/runtime/partitioner/ChannelSelector.h"
#include "streaming/runtime/streamrecord/StreamElementSerializer.h"

/**
 * Filters records for ambiguous channel mappings.
 *
 * For example, when the downstream node of a keyed exchange is scaled from 1 to 2, the state of
 * the output side on the upstream node needs to be replicated to both channels. This filter then
 * checks the deserialized records on both downstream subtasks and filters out the irrelevant
 * records.
 *
 * @tparam T The type of the record payload.
 */
namespace omnistream {
class RecordFilter : public StreamRecord {
public:
    RecordFilter(
        omnistream::datastream::ChannelSelector<IOReadableWritable>* partitioner,
        TypeSerializer* inputSerializer,
        int subtaskIndex)
        : partitioner(partitioner),
          subtaskIndex(subtaskIndex)
    {
        // Initialize the delegate with the serializer.
        delegate = new SerializationDelegate(inputSerializer);
    }

    /**
     * Tests whether the given record would have been sent to the subtask index configured
     * in this filter.
     *
     * @param streamRecord The record to test.
     * @return true if the record would have been routed to our subtask, false otherwise.
     */
    bool apply(StreamRecord& streamRecord) const
    {
        auto* obj = dynamic_cast<Object*>(&streamRecord);
        if (!obj) {
            LogError("error,streamRecord cast to obj fail!");
        }
        delegate->setInstance(obj);
        return partitioner->selectChannel(delegate) == subtaskIndex;
    }

    /**
     * Returns a predicate that accepts all records (always returns true).
     *
     * @return A callable object that always returns true.
     */
    static auto all()
    {
        auto func = std::function<bool(const StreamRecord&)>([](const StreamRecord&) { return true; });
        return func;
    }

private:
    omnistream::datastream::ChannelSelector<IOReadableWritable>* partitioner;
    SerializationDelegate* delegate;
    int subtaskIndex;
    std::shared_ptr<omnistream::datastream::StreamElementSerializer> streamElementSerializer;
};
} // namespace omnistream

#endif // OMNISTREAM_RECORDFILTER_H
