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

#ifndef STREAMRECORDHELPER_H
#define STREAMRECORDHELPER_H

#include "table/data/vectorbatch/VectorBatch.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/data/JoinedRowData.h"
#include "StreamRecord.h"
class StreamRecordHelper {
public:
    /**
     * Returns the in-memory size of the payload carried by the record.
     * A record either carries a VectorBatch or a BinaryRowData, so the value type has to be
     * honored: casting a BinaryRowData to a VectorBatch reads unrelated memory and yields a
     * bogus size (which then makes the buffer pool request an impossible amount of memory).
     */
    static uint64_t getRecordSizeInBytes(const StreamRecord* record)
    {
        if (record == nullptr) {
            return 0;
        }
        auto* value = record->getValue();
        if (value == nullptr) {
            return 0;
        }
        switch (record->getValueType()) {
            case RecordValueType::BINARY_ROW_DATA: {
                auto* row = static_cast<const BinaryRowData*>(value);
                return static_cast<uint64_t>(row->getSizeInBytes());
            }
            case RecordValueType::JOINED_ROW_DATA: {
                auto* row = const_cast<JoinedRowData*>(static_cast<const JoinedRowData*>(value));
                return static_cast<uint64_t>(row->getArity()) * sizeof(int64_t);
            }
            case RecordValueType::VECTOR_BATCH: {
                auto* vectorBatch = static_cast<const omnistream::VectorBatch*>(value);
                return static_cast<uint64_t>(vectorBatch->getSizeInBytes());
            }
            default: return 0;
        }
    }

    static StreamRecord* deepCopyVectorBatch(StreamRecord* origin)
    {
        auto record = origin;
        long timestamp = record->getTimestamp();
        auto value = record->getValue();
        auto vectorBatch = reinterpret_cast<omnistream::VectorBatch*>(value);
        auto copiedVectorBatch = vectorBatch->copy();
        auto newRecord = new StreamRecord(copiedVectorBatch, timestamp);
        newRecord->setValueType(record->getValueType());
        if (record->hasTimestamp()) {
            newRecord->setTag(StreamElementTag::TAG_REC_WITH_TIMESTAMP);
        } else {
            newRecord->setTag(StreamElementTag::TAG_REC_WITHOUT_TIMESTAMP);
        }
        return newRecord;
    }

    static StreamRecord* deepCopyObject(StreamRecord* origin)
    {
        auto record = origin;
        long timestamp = record->getTimestamp();
        auto value = record->getValue();
        auto object = reinterpret_cast<Object*>(value);
        auto copiedObject = object->clone();
        auto newRecord = new StreamRecord(copiedObject, timestamp);
        newRecord->setValueType(record->getValueType());
        if (record->hasTimestamp()) {
            newRecord->setTag(StreamElementTag::TAG_REC_WITH_TIMESTAMP);
        } else {
            newRecord->setTag(StreamElementTag::TAG_REC_WITHOUT_TIMESTAMP);
        }
        return newRecord;
    }
};

#endif
