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

#include <stdexcept>
#include <utility>
#include <vector>
#include "table/data/vectorbatch/VectorBatch.h"
#include "table/data/RowData.h"
#include "table/data/RowKind.h"
#include "table/runtime/generated/NamespaceAggsHandleFunction.h"
#include "core/typeutils/TypeSerializer.h"
#include "table/types/logical/LogicalType.h"
#include "table/data/JoinedRowData.h"
#include "table/typeutils/BinaryRowDataSerializer.h"
#include "table/runtime/operators/window/WindowOperator.h"
#include "streaming/api/operators/AbstractStreamOperator.h"

template <typename K, typename W>
class AggregateWindowOperator : public WindowOperator<K, W> {
public:
    std::unique_ptr<NamespaceAggsHandleFunction<W>> initNamespaceAggsHandleFunction(const nlohmann::json& aggInfoList);

    AggregateWindowOperator(nlohmann::json description, Output* output) : WindowOperator<K, W>(description, output)
    {
        this->output = output;
        this->collector = std::make_unique<TimestampedCollector>(output);

        for (const auto& typeStr : WindowOperator<K, W>::outputTypes) {
            outputTypeIds.push_back(LogicalType::flinkTypeToOmniTypeId(typeStr));
        }

        if (description.contains("aggInfoList") && !description["aggInfoList"].empty()) {
            aggWindowAggregator = initNamespaceAggsHandleFunction(description["aggInfoList"]);
        } else {
            aggWindowAggregator = nullptr;
        }
    }

    void open()
    {
        if (aggWindowAggregator != nullptr) {
            this->windowAggregator = std::move(aggWindowAggregator);
        }
        WindowOperator<K, W>::open();
        reuseOutput_ = std::make_unique<JoinedRowData>();
    }

    void setCurrentKey(K key) override
    {
        this->stateHandler->setCurrentKey(key);
    }

    void initializeState(StreamTaskStateInitializerImpl* initializer, TypeSerializer* keySerializer) override
    {
        AbstractStreamOperator<K>::SetOperatorID(OneInputStreamOperator::GetOperatorID().toString());
        AbstractStreamOperator<K>::initializeState(initializer, keySerializer);
    };

    void notifyCheckpointComplete(long checkpointId) override
    {
        AbstractStreamOperator<K>::notifyCheckpointComplete(checkpointId);
    }

    void notifyCheckpointAborted(long checkpointId) override
    {
        AbstractStreamOperator<K>::notifyCheckpointAborted(checkpointId);
    }

protected:
    void emitWindowResult(const W& window) override;

private:
    omnistream::VectorBatch* createOutputBatch(const std::vector<RowData*>& collectedRows);

    void collect(RowKind rowKind, RowData* key, std::unique_ptr<RowData> aggResult)
    {
        reuseResultRows_.clear();
        reuseOutput_->replace(key, aggResult.get());
        reuseOutput_->setRowKind(rowKind);
        auto resultRow = std::unique_ptr<BinaryRowData>(
            BinaryRowDataSerializer::joinedRowToBinaryRow(reuseOutput_.get(), outputTypeIds));
        reuseResultRows_.push_back(resultRow.get());
        auto resultBatch = createOutputBatch(reuseResultRows_);
        collectOutputBatch(collector.get(), resultBatch);
    }

    void collectOutputBatch(TimestampedCollector* out, omnistream::VectorBatch* outputBatch)
    {
        out->collect(outputBatch);
    }

    Output* output;
    std::unique_ptr<TimestampedCollector> collector;
    std::unique_ptr<NamespaceAggsHandleFunction<W>> aggWindowAggregator;
    std::unique_ptr<JoinedRowData> reuseOutput_{};
    std::vector<RowData*> reuseResultRows_;
    std::vector<int32_t> outputTypeIds;
};
