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

#ifndef NONRECORDWRITERV2_H
#define NONRECORDWRITERV2_H

#include "RecordWriterDelegateV2.h"

namespace omnistream {
class NonRecordWriterV2 : public RecordWriterDelegateV2 {
public:
    NonRecordWriterV2() = default;

    RecordWriterV2* getRecordWriter(int outputIndex) override
    {
        throw std::invalid_argument("NonRecordWriterV2 unsupport");
    }

    void cancel() override
    {
    }
    void close() override
    {
    }
    void broadcastEvent(std::shared_ptr<AbstractEvent> event) override
    {
    }

    std::shared_ptr<CompletableFuture> GetAvailableFuture() override
    {
        THROW_LOGIC_EXCEPTION("No record writer instance");
    }

    bool isAvailable() override
    {
        return true;
    }
};
} // namespace omnistream

#endif
