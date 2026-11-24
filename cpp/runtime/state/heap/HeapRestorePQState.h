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

#pragma once

#include <string>
#include <vector>

#include "runtime/state/HeapKeyedStateBackend.h"
#include "runtime/state/restore/RestorePQState.h"

namespace omnistream {

template <typename K>
class HeapRestorePQState : public RestorePQState {
public:
    HeapRestorePQState(HeapKeyedStateBackend<K>* backend, const std::string& stateName, int keyGroupPrefixBytes)
        : backend_(backend),
          stateName_(stateName),
          keyGroupPrefixBytes_(keyGroupPrefixBytes)
    {
    }

    void writeEntry(const std::vector<int8_t>& keyBytes, const std::vector<int8_t>& /*valueBytes*/) override
    {
        backend_->addRestoredPriorityQueueEntry(stateName_, keyBytes, keyGroupPrefixBytes_);
    }

    void flush() override
    {
        // Heap writes are immediate via addRestoredPriorityQueueEntry; nothing to flush.
    }

    void discard() override
    {
        // Heap writes are immediate; nothing to discard.
    }

private:
    HeapKeyedStateBackend<K>* backend_;
    std::string stateName_;
    int keyGroupPrefixBytes_;
};

} // namespace omnistream
