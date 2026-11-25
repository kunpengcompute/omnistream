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

#ifndef OBJECTSEGMENT_H
#define OBJECTSEGMENT_H

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <utility>
#include "table/data/Row.h"
#include <streaming/runtime/streamrecord/StreamElement.h>
#include <streaming/runtime/streamrecord/StreamRecord.h>
#include <streaming/api/watermark/Watermark.h>
#include "runtime/watermark/WatermarkStatus.h"
#include <vector/vector.h>

#include "table/data/vectorbatch/VectorBatch.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/data/JoinedRowData.h"
#include "basictypes/Object.h"
#include "basictypes/SizeAwareObject.h"
#include "core/memory/Segment.h"

namespace omnistream {
class ObjectSegmentChannelStateSerde;

class ObjectSegment : public Segment, public SizeAwareObject {
public:
    explicit ObjectSegment(size_t size) : Segment(SegmentType::OBJECT_SEGMENT), size(size)
    {
        objects_ = new StreamElement*[size]();
        countCreated();
    }

    ~ObjectSegment() override
    {
        countDestroyed();
        delete[] objects_;
    }

    int putObject(int offset, StreamElement* record)
    {
        LOG("objects address" << objects_[offset]);
        LOG("objects size()" << size);
        objects_[offset] = record;
        countStored();
        sizeInBytes_ += calculateStoredObjectSizeInBytes(record);
        return 1; // written size
    }

    void put(int index, const ObjectSegment* src, int offset, int length)
    {
        if (src == nullptr) {
            throw std::invalid_argument("Source ObjectSegment is null");
        }
        if (index < 0 || offset < 0 || length < 0 || static_cast<size_t>(index) + static_cast<size_t>(length) > size ||
            static_cast<size_t>(offset) + static_cast<size_t>(length) > src->size) {
            throw std::out_of_range("ObjectSegment copy range out of bounds");
        }
        int copied = 0;
        try {
            for (; copied < length; copied++) {
                objects_[index + copied] = CloneObject(src->objects_[offset + copied]);
                countStored();
                sizeInBytes_ += calculateStoredObjectSizeInBytes(objects_[index + copied]);
            }
            ownsObjects_ = true;
        } catch (...) {
            for (int i = 0; i < copied; i++) {
                ReleaseObject(objects_[index + i]);
                objects_[index + i] = nullptr;
            }
            throw;
        }
    }

    void ReleaseObjects()
    {
        if (!ownsObjects_) {
            return;
        }
        for (size_t i = 0; i < size; i++) {
            ReleaseObject(objects_[i]);
            objects_[i] = nullptr;
        }
        ownsObjects_ = false;
    }

    StreamElement* getObject(int offset)
    {
        return objects_[offset];
    }

    [[nodiscard]] size_t getSize() const
    {
        return size;
    }

    [[nodiscard]] int64_t getObjectSizeInBytes() const override;
    void reset();
    int64_t getCapacity();
    void setCapacity(int64_t capacity);

    static int64_t calculateStoredObjectSizeInBytes(const StreamElement* record);
    static void countDrained();
    static void reportCounters(const char* where);

    void setData(uint8_t* bufferAddress, size_t bufferLength = 0)
    {
        offHeapBuffer_ = bufferAddress;
        offHeapBufferLength_ = bufferLength;
    }

    uint8_t* getData()
    {
        return offHeapBuffer_;
    }

    size_t getDataLength() const
    {
        return offHeapBufferLength_;
    }

private:
    friend class ObjectSegmentChannelStateSerde;

    size_t size;
    int64_t sizeInBytes_ = 0;
    int64_t capacityInBytes_ = 0;
    bool ownsObjects_ = false;

    // These fields point to the immutable serialized payload owned by the Java network Buffer. The
    // VectorBatchBuffer reference count keeps that Buffer alive until all consumers and checkpoint writers recycle it.
    // This is for RemoteInputChannel::notifyRemoteDataAvailableForVectorBatch.
    // When VectorBatchBuffer from RemoteInputChannel should be recycled, OriginalNetworkBufferRecycler needs to push
    // offHeapBuffer_ to originalNetworkBufferQueue, making the buffer recycler thread in
    // RemoteDataFetcher(OmniAdaptor) could correctly recycle the buffer.
    uint8_t* offHeapBuffer_ = nullptr;
    size_t offHeapBufferLength_ = 0;

    //  it is actually a  StreamRecord * [size] , allocate mem in constructor, StreamRecord.value are VectorBatch *
    //  notice in order to get high performance, the data related object are using raw pointer
    StreamElement** objects_;

    static void countCreated();
    static void countDestroyed();
    static void countStored();

    void ReleaseObjects(size_t offset, size_t length) noexcept
    {
        for (size_t i = 0; i < length; i++) {
            ReleaseObject(objects_[offset + i]);
            objects_[offset + i] = nullptr;
        }
    }

    static StreamElement* CloneObject(StreamElement* source)
    {
        if (source == nullptr) {
            return nullptr;
        }

        StreamElementTag tag = source->getTag();
        if (tag == StreamElementTag::TAG_UNKNOWN) {
            INFO_RELEASE("Warn: CloneObject tag is TAG_UNKNOWN");
            return nullptr;
        }
        if (tag == StreamElementTag::TAG_STREAM_STATUS) {
            return new WatermarkStatus(static_cast<WatermarkStatus*>(source)->GetStatus());
        }
        if (tag == StreamElementTag::TAG_RECORD_ATTRIBUTES || tag == StreamElementTag::TAG_LATENCY_MARKER ||
            tag == StreamElementTag::TAG_INTERNAL_WATERMARK) {
            ERROR_RELEASE("CloneObject does not support StreamElement tag " << static_cast<int>(tag));
            throw std::runtime_error(
                "CloneObject does not support StreamElement tag " + std::to_string(static_cast<int>(tag)));
        }
        if (tag == StreamElementTag::TAG_WATERMARK) {
            auto* watermark = dynamic_cast<Watermark*>(source);
            if (watermark == nullptr) {
                INFO_RELEASE("Error: CloneObject watermark is nullptr.");
                return nullptr;
            }
            return new Watermark(watermark->getTimestamp());
        }

        if (tag == StreamElementTag::TAG_REC_WITH_TIMESTAMP || tag == StreamElementTag::TAG_REC_WITHOUT_TIMESTAMP ||
            tag == StreamElementTag::VECTOR_BATCH) {
            if (auto* record = dynamic_cast<StreamRecord*>(source); record != nullptr && record->hasExternalRow()) {
                INFO_RELEASE("Warn: CloneObject StreamRecord hasExternalRow, which is not supported.");
                throw std::invalid_argument("CloneObject StreamRecord hasExternalRow, which is not supported.");
            }
            if (auto* record = dynamic_cast<StreamRecord*>(source);
                record != nullptr && record->getValueType() == RecordValueType::BINARY_ROW_DATA) {
                // A binary row record must not be treated as a vector batch, its payload is a row.
                auto* row = static_cast<BinaryRowData*>(record->getValue());
                auto* copiedRow = row->copy();
                StreamRecord* copiedRecord = record->hasTimestamp()
                                                 ? new StreamRecord(copiedRow, record->getTimestamp())
                                                 : new StreamRecord(copiedRow);
                copiedRecord->setValueType(record->getValueType());
                copiedRecord->setExternalRow(record->hasExternalRow());
                copiedRecord->setTag(tag);
                return copiedRecord;
            }
            if (auto* record = dynamic_cast<StreamRecord*>(source);
                record != nullptr && record->getValueType() == RecordValueType::JOINED_ROW_DATA) {
                throw std::invalid_argument("CloneObject requires JOINED_ROW_DATA to be materialized");
            }
            auto* vectorBatch = static_cast<VectorBatch*>(source->getValue());
            if (vectorBatch == nullptr) {
                INFO_RELEASE("Warn: CloneObject getValue is not VectorBatch.");
                if (static_cast<Row*>(source->getValue())) {
                    INFO_RELEASE("Warn: CloneObject getValue is Row.");
                }
                return nullptr;
            }
            auto* copiedVectorBatch = vectorBatch->copy();
            if (auto* record = dynamic_cast<StreamRecord*>(source)) {
                StreamRecord* copiedRecord = record->hasTimestamp()
                                                 ? new StreamRecord(copiedVectorBatch, record->getTimestamp())
                                                 : new StreamRecord(copiedVectorBatch);
                copiedRecord->setExternalRow(record->hasExternalRow());
                copiedRecord->setTag(tag);
                return copiedRecord;
            }

            auto* copiedElement = new StreamElement(StreamElementTag::VECTOR_BATCH);
            copiedElement->setValue(copiedVectorBatch);
            return copiedElement;
        }

        ERROR_RELEASE("CloneObject encountered unsupported StreamElement tag " << static_cast<int>(tag));
        throw std::runtime_error(
            "CloneObject encountered unsupported StreamElement tag " + std::to_string(static_cast<int>(tag)));
    }

    /**
     * Deletes the payload of a record with the type it actually has. Deleting a binary row as a
     * vector batch (or the other way round) corrupts the heap.
     */
    static void ReleaseRecordValue(StreamRecord* record)
    {
        if (record == nullptr) {
            return;
        }
        void* value = record->getValue();
        if (value == nullptr) {
            return;
        }
        switch (record->getValueType()) {
            case RecordValueType::BINARY_ROW_DATA: delete static_cast<BinaryRowData*>(value); break;
            case RecordValueType::JOINED_ROW_DATA: delete static_cast<JoinedRowData*>(value); break;
            case RecordValueType::VECTOR_BATCH: delete static_cast<VectorBatch*>(value); break;
            case RecordValueType::OBJECT: delete static_cast<Object*>(value); break;
            default: break;
        }
    }

    static void ReleaseObject(StreamElement* object)
    {
        if (object == nullptr) {
            return;
        }

        StreamElementTag tag = object->getTag();
        if (tag == StreamElementTag::TAG_REC_WITH_TIMESTAMP || tag == StreamElementTag::TAG_REC_WITHOUT_TIMESTAMP ||
            tag == StreamElementTag::VECTOR_BATCH) {
            if (auto* record = dynamic_cast<StreamRecord*>(object)) {
                if (!record->hasExternalRow()) {
                    ReleaseRecordValue(record);
                }
                delete record;
                return;
            }
            delete static_cast<VectorBatch*>(object->getValue());
            delete object;
            return;
        }

        if (tag == StreamElementTag::TAG_WATERMARK) {
            if (auto* watermark = dynamic_cast<Watermark*>(object)) {
                delete watermark;
            } else {
                delete object;
            }
            return;
        }

        delete object;
    }
};
} // namespace omnistream

#endif
