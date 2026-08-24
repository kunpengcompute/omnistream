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

#ifndef COPYINGBROADCASTINGOUTPUTCOLLECTOR_H
#define COPYINGBROADCASTINGOUTPUTCOLLECTOR_H

#include <streaming/runtime/streamrecord/StreamRecordHelper.h>
#include "WatermarkGaugeExposingOutput.h"
#include "table/data/Row.h"

namespace omnistream {
class VectorBatchCopyingBroadcastingOutputCollector : public WatermarkGaugeExposingOutput {
public:
    explicit VectorBatchCopyingBroadcastingOutputCollector(std::vector<WatermarkGaugeExposingOutput*> outputs)
        : outputs(std::move(outputs)) {};

    void collect(void* record) override
    {
        auto streamRecord = reinterpret_cast<StreamRecord*>(record);
        long timestamp = streamRecord->getTimestamp();
        auto value = streamRecord->getValue();

        LOG(" VectorBatchCopyingBroadcastingOutputCollector collect >>>>>>>>");
        for (size_t i = 0; i < outputs.size() - 1; i++) {
            auto output = outputs[i];
            if (streamRecord->hasExternalRow()) {
                auto row = reinterpret_cast<Row*>(value);
                auto copyRow =
                    new Row(row->getKind(), row->getFieldByPosition(), row->getFieldByName(), row->getPositionByName());
                auto copyStreamRecord = new StreamRecord(copyRow, timestamp);
                copyStreamRecord->setExternalRow(true);
                copyStreamRecord->setTag(streamRecord->getTag());
                output->collect(copyStreamRecord);
                delete row;
                delete streamRecord;
            } else {
                if (streamRecord->getValueType() == RecordValueType::JOINED_ROW_DATA) {
                    LOG("collect JOINED_ROW_DATA");//TODO: JoinROWDATA havent been fully supported.
                    auto copyRow = BinaryRowDataSerializer::joinedRowToBinaryRow(reinterpret_cast<JoinedRowData*>(value));
                    auto copyStreamRecord = new StreamRecord(copyRow, timestamp);
                    copyStreamRecord->setTag(streamRecord->getTag());
                    copyStreamRecord->setValueType(streamRecord->getValueType());
                    output->collect(copyStreamRecord);
                } else if (streamRecord->getValueType() == RecordValueType::BINARY_ROW_DATA) {
                    LOG("collect binary row data");
                    auto copyRow = reinterpret_cast<BinaryRowData*>(value)->copy();
                    auto copyStreamRecord = new StreamRecord(copyRow, timestamp);
                    copyStreamRecord->setTag(streamRecord->getTag());
                    copyStreamRecord->setValueType(streamRecord->getValueType());
                    output->collect(copyStreamRecord);
                } else { //vectorbatch
                    LOG("collect vector batch");
                    StreamRecord *copyRecord = StreamRecordHelper::deepCopyVectorBatch(streamRecord);
                    copyRecord->setTag(streamRecord->getTag());
                    copyRecord->setValueType(streamRecord->getValueType());
                    output->collect(copyRecord);
                }
            }
        }
        if (!outputs.empty()) {
            outputs[outputs.size() - 1]->collect(record);
        }
    }

    void close() override
    {
    }

    void emitWatermark(Watermark* watermark) override
    {
        for (auto output : outputs) {
            output->emitWatermark(watermark);
        }
    }

    void emitWatermarkStatus(WatermarkStatus* watermarkStatus) override
    {
        for (auto output : outputs) {
            output->emitWatermarkStatus(watermarkStatus);
        }
    }

private:
    std::vector<WatermarkGaugeExposingOutput*> outputs;
};
} // namespace omnistream

namespace omnistream::datastream {
class CopyingBroadcastingOutputCollector : public WatermarkGaugeExposingOutput {
public:
    explicit CopyingBroadcastingOutputCollector(std::vector<WatermarkGaugeExposingOutput*> outputs)
        : outputs(std::move(outputs)) {};

    void collect(void* record) override
    {
        LOG(" CopyingBroadcastingOutputCollector collect >>>>>>>>");
        if (outputs.empty()) {
            auto streamRecord = reinterpret_cast<StreamRecord*>(record);
            Object* value = reinterpret_cast<Object*>(streamRecord->getValue());
            value->putRefCount();
            return;
        }

        for (size_t i = 0; i < outputs.size() - 1; i++) {
            auto streamRecord = reinterpret_cast<StreamRecord*>(record);
            StreamRecord* copyRecord = StreamRecordHelper::deepCopyObject(streamRecord);
            outputs[i]->collect(copyRecord);
            delete copyRecord;
        }

        if (!outputs.empty()) {
            outputs[outputs.size() - 1]->collect(record);
        }
    }

    void close() override
    {
    }

    void emitWatermark(Watermark* watermark) override
    {
        for (auto output : outputs) {
            output->emitWatermark(watermark);
        }
    }

    void emitWatermarkStatus(WatermarkStatus* watermarkStatus) override
    {
        for (auto output : outputs) {
            output->emitWatermarkStatus(watermarkStatus);
        }
    }

private:
    std::vector<WatermarkGaugeExposingOutput*> outputs;
};
} // namespace omnistream::datastream
#endif // COPYINGBROADCASTINGOUTPUTCOLLECTOR_H
