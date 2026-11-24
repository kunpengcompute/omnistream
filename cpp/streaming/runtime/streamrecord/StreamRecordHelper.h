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
#include "StreamRecord.h"
class StreamRecordHelper {
public:
    static StreamRecord* deepCopyVectorBatch(StreamRecord* origin)
    {
        auto record = origin;
        long timestamp = record->getTimestamp();
        auto value = record->getValue();
        auto vectorBatch = reinterpret_cast<omnistream::VectorBatch*>(value);
        auto copiedVectorBatch = vectorBatch->copy();
        return new StreamRecord(copiedVectorBatch, timestamp);
    }

    static StreamRecord* deepCopyObject(StreamRecord* origin)
    {
        auto record = origin;
        long timestamp = record->getTimestamp();
        auto value = record->getValue();
        auto object = reinterpret_cast<Object*>(value);
        auto copiedObject = object->clone();
        return new StreamRecord(copiedObject, timestamp);
    }
};

#endif
