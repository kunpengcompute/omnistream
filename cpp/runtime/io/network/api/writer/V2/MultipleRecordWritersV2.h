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

#ifndef MULTIPLERECORDWRITERSV2_H
#define MULTIPLERECORDWRITERSV2_H
#include "RecordWriterDelegateV2.h"

#include <memory>

namespace omnistream {
class MultipleRecordWritersV2 : public RecordWriterDelegateV2 {
public:
    // Takes ownership of every writer: each owns an OutputFlusher thread that is only stopped by
    // ~OutputFlusher, so the writers must be destroyed with the delegate or the threads leak.
    explicit MultipleRecordWritersV2(std::vector<RecordWriterV2*>& recordWriters);

    RecordWriterV2* getRecordWriter(int outputIndex) override;

    ~MultipleRecordWritersV2() override
    {
        INFO_RELEASE("~MultipleRecordWritersV2 with " << recordWriters.size() << " writers");
    }

    void close() override;

    void cancel() override;

    void broadcastEvent(std::shared_ptr<AbstractEvent> event) override
    {
        for (auto& writer : recordWriters) {
            writer->broadcastEvent(event);
        }
    }

    std::shared_ptr<CompletableFuture> GetAvailableFuture() override
    {
        for (int i = 0; i < recordWriters.size(); i++) {
            futures.push_back(recordWriters[i]->GetAvailableFuture());
        }
        return CompletableFuture::allOf(futures);
    }

    bool isAvailable() override
    {
        for (auto& recordWriter : recordWriters) {
            if (!recordWriter->isAvailable()) {
                return false;
            }
        }
        return true;
    }

private:
    std::vector<std::unique_ptr<RecordWriterV2>> recordWriters;
    std::vector<std::shared_ptr<CompletableFuture>> futures;
};
} // namespace omnistream

#endif
