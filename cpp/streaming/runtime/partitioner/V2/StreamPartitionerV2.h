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

#ifndef STREAMPARTITIONER_V2_H
#define STREAMPARTITIONER_V2_H

#include <string>
#include "ChannelSelectorV2.h"
#include "runtime/io/network/api/writer/SubtaskStateMapper.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "table/data/Row.h"
#include "table/data/util/VectorBatchUtil.h"
#include "table/data/JoinedRowData.h"
#include "table/data/binary/BinaryRowData.h"

namespace omnistream {
template <typename T>
class StreamPartitionerV2 : public ChannelSelectorV2<T> {
public:
    StreamPartitionerV2() : numberOfChannels(0)
    {
    }

    virtual ~StreamPartitionerV2()
    {
    }

    void setup(int numberOfChannels_) override
    {
        this->numberOfChannels = numberOfChannels_;
    }

    [[nodiscard]] bool isBroadcast() const override
    {
        return false;
    }
    virtual int selectRowChannel(omnistream::VectorBatch* record, int rowIndex) = 0;

    virtual int selectRowChannel(RowData* record) = 0;

    std::unordered_map<int, T*> selectChannel(T* record) override
    {
        LOG("StreamPartitionerV2 selectChannel >>>>>>>>");
        std::unordered_map<int, T*> result;
        std::unordered_map<int, std::vector<int32_t>> intermediateResult;
        auto streamRecord = reinterpret_cast<StreamRecord*>(record);
        if (streamRecord->hasExternalRow()) {
            auto row = static_cast<Row*>(streamRecord->getValue());
            long timestamp = streamRecord->getTimestamp();

            auto channel = selectRowChannel(row->getInternalRow());
            intermediateResult[channel].push_back(0);

            for (auto& pair : intermediateResult) {
                result[pair.first] = buildNewStreamRecordBasedOnOffsets(streamRecord, timestamp);
            }

            delete row->getInternalRow();
            delete row;
            delete streamRecord;
            return result;
        } else if (streamRecord->getValueType() == RecordValueType::JOINED_ROW_DATA) {
            // do we need to handle joined Rowdata?
            auto row = static_cast<JoinedRowData*>(streamRecord->getValue());
            auto channel = selectRowChannel(row);
            result[channel] = streamRecord;
            return result;
        } else if (streamRecord->getValueType() == RecordValueType::BINARY_ROW_DATA) {
            auto row = static_cast<BinaryRowData*>(streamRecord->getValue());
            auto channel = selectRowChannel(row);
            result[channel] = streamRecord; // TODO: copy the row using buildNewStreamRecordBasedOnOffsets().

            return result;
        }
        auto vectorBatch = static_cast<VectorBatch*>(streamRecord->getValue());

        long timestamp = streamRecord->getTimestamp();

        size_t row_cnt = vectorBatch->GetRowCount();
        if (row_cnt == 1) {
            auto channel = selectRowChannel(vectorBatch, 0);
            result[channel] = streamRecord;
            return result;
        }
        for (size_t i = 0; i < row_cnt; i++) {
            auto channel = selectRowChannel(vectorBatch, i);
            intermediateResult[channel].push_back(i);
        }

        // convert intermediateResult to result
        for (auto& pair : intermediateResult) {
            result[pair.first] = buildNewStreamRecordBasedOnOffsets(pair.second, streamRecord, timestamp);
        }

        delete vectorBatch;
        delete streamRecord;
        return result;
    }

    virtual std::unique_ptr<StreamPartitionerV2<T>> copy() = 0;
    virtual std::unique_ptr<SubtaskStateMapper> getDownstreamSubtaskStateMapper() = 0;
    virtual bool isPointwise() = 0;
    [[nodiscard]] virtual std::string toString() const = 0;
    virtual std::unique_ptr<SubtaskStateMapper> getUpstreamSubtaskStateMapper() = 0;

    StreamRecord* buildNewStreamRecordBasedOnOffsets(
        std::vector<int>& offsets, StreamRecord* originStreamRecord, long timestamp)
    {
        auto vectorBatch = reinterpret_cast<VectorBatch*>(originStreamRecord->getValue());
        auto copyedVectorBatch = VectorBatchUtil::buildNewVectorBatchByRowIds(vectorBatch, offsets);
        auto copyedRecord = new StreamRecord(copyedVectorBatch, timestamp);
        copyedRecord->setValueType(originStreamRecord->getValueType());
        return copyedRecord;
    }

    StreamRecord* buildNewStreamRecordBasedOnOffsets(StreamRecord* originStreamRecord, long timestamp)
    {
        auto row = reinterpret_cast<Row*>(originStreamRecord->getValue());
        auto copyRow =
            new Row(row->getKind(), row->getFieldByPosition(), row->getFieldByName(), row->getPositionByName());
        return new StreamRecord(copyRow, timestamp);
    }

protected:
    int numberOfChannels;
};
} // namespace omnistream

#endif
