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

#ifndef FLINK_TNEL_SINGLERECORDWRITER_V2_H
#define FLINK_TNEL_SINGLERECORDWRITER_V2_H

#include "../include/common.h"
#include "RecordWriterDelegateV2.h"

#include <memory>

namespace omnistream {
class SingleRecordWriterV2 : public RecordWriterDelegateV2 {
public:
    // Takes ownership: the writer owns an OutputFlusher thread that is only stopped by
    // ~OutputFlusher, so the writer must be destroyed with the delegate or the thread leaks.
    explicit SingleRecordWriterV2(RecordWriterV2* recordWriter);

    RecordWriterV2* getRecordWriter(int outputIndex) override;

    ~SingleRecordWriterV2() override
    {
        INFO_RELEASE("~SingleRecordWriterV2");
    }

    void cancel() override;
    void close() override;

    void broadcastEvent(std::shared_ptr<AbstractEvent> event) override
    {
        recordWriter_->broadcastEvent(event);
    }

    std::shared_ptr<CompletableFuture> GetAvailableFuture() override
    {
        return recordWriter_->GetAvailableFuture();
    }

    bool isAvailable() override
    {
        return recordWriter_->isAvailable();
    }

private:
    std::unique_ptr<RecordWriterV2> recordWriter_;
};
} // namespace omnistream

#endif
