/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#ifndef FLINK_TNEL_DROPUPDATEBEFOREOPERATOR_H
#define FLINK_TNEL_DROPUPDATEBEFOREOPERATOR_H

#include <memory>
#include <nlohmann/json.hpp>
#include <vector>

#include "core/include/common.h"
#include "streaming/api/operators/AbstractStreamOperator.h"
#include "streaming/api/operators/OneInputStreamOperator.h"
#include "streaming/api/operators/TimestampedCollector.h"
#include "table/data/RowKind.h"
#include "table/data/util/VectorBatchUtil.h"
#include "table/data/vectorbatch/VectorBatch.h"

namespace omnistream {

/**
 * Native DropUpdateBefore operator.
 *
 * Stateless row-kind filter that drops only rows with RowKind::UPDATE_BEFORE and forwards every
 * other row unchanged (INSERT, UPDATE_AFTER, DELETE), preserving row order, per-row timestamps and
 * row kinds. This matches the semantics of Flink's
 * org.apache.flink.table.runtime.operators.misc.DropUpdateBeforeFunction.
 */
class DropUpdateBeforeOperator : public OneInputStreamOperator, public AbstractStreamOperator<int> {
public:
    explicit DropUpdateBeforeOperator(const nlohmann::json& description, Output* output) : description_(description)
    {
        this->setOutput(output);
        LOG("DropUpdateBeforeOperator description: " << description);
    }

    ~DropUpdateBeforeOperator() override = default;

    void processBatch(StreamRecord* input) override
    {
        // The operator takes ownership of the incoming StreamRecord and must release it on every
        // path (see StreamCalcBatch.cpp and KeyedProcessOperator). The VectorBatch it wraps is
        // released or forwarded separately below.
        auto inputGuard = std::unique_ptr<StreamRecord>(input);
        // Align with Flink's StreamFilter, which forwards the original element and therefore keeps
        // its record-level timestamp. Both collect paths below must carry this timestamp.
        timestampedCollector_->setTimestamp(input);
        auto* record = reinterpret_cast<VectorBatch*>(input->getValue());
        const int32_t rowCount = record->GetRowCount();
        if (rowCount <= 0) {
            delete record;
            return;
        }
        RowKind* rowKinds = record->getRowKinds();
        std::vector<int32_t> keptRowIds;
        keptRowIds.reserve(rowCount);
        for (int32_t i = 0; i < rowCount; ++i) {
            if (rowKinds[i] != RowKind::UPDATE_BEFORE) {
                keptRowIds.push_back(i);
            }
        }
        if (keptRowIds.empty()) {
            // All rows are UPDATE_BEFORE: emit nothing.
            delete record;
            return;
        }
        if (static_cast<size_t>(rowCount) == keptRowIds.size()) {
            // Nothing to drop: forward the original batch as-is.
            timestampedCollector_->collect(record);
            return;
        }
        auto* outputBatch = VectorBatchUtil::buildNewVectorBatchByRowIds(record, keptRowIds);
        timestampedCollector_->collect(outputBatch);
        delete record;
    }

    void processElement(StreamRecord* record) override
    {
        NOT_IMPL_EXCEPTION;
    }

    void open() override
    {
        INFO_RELEASE("DropUpdateBefore descriptionJson: " << description_.dump());
        timestampedCollector_ = new TimestampedCollector(this->output);
    }

    void close() override
    {
        if (timestampedCollector_ != nullptr) {
            timestampedCollector_->close();
            delete timestampedCollector_;
            timestampedCollector_ = nullptr;
        }
        AbstractStreamOperator<int>::close();
    }

    const char* getName() override
    {
        return "DropUpdateBefore";
    }

    void initializeState(StreamTaskStateInitializerImpl* initializer, TypeSerializer* keySerializer) override
    {
        LOG("DropUpdateBeforeOperator initializeState()");
        // DropUpdateBefore is stateless, so there is nothing to initialize.
    }

    void ProcessWatermark(Watermark* watermark) override
    {
        output->emitWatermark(watermark);
    }

    void processWatermarkStatus(WatermarkStatus* watermarkStatus) override
    {
        output->emitWatermarkStatus(watermarkStatus);
    }

    std::string getTypeName() override
    {
        std::string typeName = "DropUpdateBefore";
        typeName.append(__PRETTY_FUNCTION__);
        return typeName;
    }

    std::shared_ptr<omnistream::TaskMetricGroup> GetMectrics() override
    {
        return AbstractStreamOperator::GetMectrics();
    }

private:
    nlohmann::json description_;
    TimestampedCollector* timestampedCollector_ = nullptr;
};

} // namespace omnistream

#endif // FLINK_TNEL_DROPUPDATEBEFOREOPERATOR_H
