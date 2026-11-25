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

#ifndef WINDOWVALUESTATE_H
#define WINDOWVALUESTATE_H

#include "WindowState.h"
#include "runtime/state/rocksdb/RocksdbValueState.h"
#include "state/internal/InternalValueState.h"

template <typename KeyType, typename W, typename ValType>
class WindowValueState : public WindowState<W> {
public:
    WindowValueState(InternalValueState<KeyType, W, ValType>* windowState) : windowState(windowState)
    {
        isFalconEnabled_ = dynamic_cast<RocksdbValueState<KeyType, W, ValType>*>(windowState) != nullptr &&
                           dynamic_cast<RocksdbValueState<KeyType, W, ValType>*>(windowState)->isFalconEnabled();
    }

    void clear(W window) override
    {
        windowState->setCurrentNamespace(window);
        windowState->clear();
    };

    ValType value(W window)
    {
        windowState->setCurrentNamespace(window);
        return windowState->value();
    };

    void update(W window, ValType value)
    {
        windowState->setCurrentNamespace(window);
        windowState->update(value);
    };

    ~WindowValueState() override
    {
        delete windowState;
    };

    void addVectorBatch(int32_t keyGroup, omnistream::VectorBatch* batch)
    {
        windowState->addVectorBatch(keyGroup, batch);
    };

    void addVectorBatches(const std::unordered_map<int32_t, omnistream::VectorBatch*>& vectorBatchByKeyGroup)
    {
        windowState->addVectorBatches(vectorBatchByKeyGroup);
    }

    std::vector<omnistream::VectorBatch*> getVectorBatches(int32_t keyGroup)
    {
        return windowState->getVectorBatches(keyGroup);
    };

    uint32_t getCurrentBatchId(int32_t keyGroup)
    {
        return windowState->getNextSequenceNumber(keyGroup);
    };

    bool isFalconEnabled() const
    {
        return isFalconEnabled_;
    }

private:
    InternalValueState<KeyType, W, ValType>* windowState;
    bool isFalconEnabled_ = false;
};

#endif
