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

#ifndef OMNISTREAM_ROCKSTRANSFORMINGITERATORWRAPPER_H
#define OMNISTREAM_ROCKSTRANSFORMINGITERATORWRAPPER_H
#include <vector>
#include <functional>
#include <stdexcept>
#include "runtime/state/RocksIteratorWrapper.h"
#include "runtime/state/StateSnapshotTransformer.h"

/**
 * Wrapper around RocksIterator that applies a given StateSnapshotTransformer to the
 * elements during the iteration.
 */
class RocksTransformingIteratorWrapper : public RocksIteratorWrapper {
public:
    /**
     * Constructs a transforming iterator wrapper.
     *
     * @param iterator The base RocksDB iterator to wrap
     * @param transformer State snapshot transformer to apply to values
     */
    RocksTransformingIteratorWrapper(
        std::unique_ptr<rocksdb::Iterator> iterator, StateSnapshotTransformer<std::vector<int8_t>>* transformer)
        : RocksIteratorWrapper(std::move(iterator)),
          stateSnapshotTransformer_(transformer)
    {
        if (!transformer) {
            throw std::invalid_argument("StateSnapshotTransformer cannot be null");
        }
    }

    void seekToFirst()
    {
        RocksIteratorWrapper::seekToFirst();
    }

    void seekToLast()
    {
        RocksIteratorWrapper::seekToLast();
    }

    void next()
    {
        RocksIteratorWrapper::next();
    }

    void prev()
    {
        RocksIteratorWrapper::prev();
    }

    std::vector<int8_t> value()
    {
        if (!isValid()) {
            throw std::runtime_error("value() called on invalid iterator");
        }
        return currentValue_;
    }

private:
    StateSnapshotTransformer<std::vector<int8_t>>* stateSnapshotTransformer_;
    std::vector<int8_t> currentValue_;
};
#endif // OMNISTREAM_ROCKSTRANSFORMINGITERATORWRAPPER_H
