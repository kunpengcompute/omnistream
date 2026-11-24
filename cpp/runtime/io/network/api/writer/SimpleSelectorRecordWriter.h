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

#ifndef SIMPLESELECTORRECORDWRITER_H
#define SIMPLESELECTORRECORDWRITER_H
#include <streaming/runtime/streamrecord/StreamRecord.h>

#include <streaming/runtime/partitioner/V2/ChannelSelectorV2.h>
#include "streaming/api/watermark/Watermark.h"
#include "runtime/io/network/api/writer/V2/RecordWriterV2.h"

namespace omnistream {

class SimpleSelectorRecordWriter : public RecordWriterV2 {
public:
    SimpleSelectorRecordWriter(
        std::shared_ptr<ResultPartitionWriter> writer, // Raw pointer
        ChannelSelectorV2<StreamRecord>* channelSelector,
        long timeout,
        const std::string& taskName,
        int taskType);

    ~SimpleSelectorRecordWriter() = default;

    void emit(StreamRecord* record) override;

    void broadcastEmit(Watermark* watermark) override;

protected:
    void emit(StreamRecord* record, int targetSubpartition) override;

private:
    ChannelSelectorV2<StreamRecord>* channelSelector; // Raw pointer
};

} // namespace omnistream

#endif
