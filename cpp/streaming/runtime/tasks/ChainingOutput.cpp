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

#include "ChainingOutput.h"
#include "table/data/RowData.h"
#include "metrics/groups/TaskMetricGroup.h"
#include "streaming/api/operators/AbstractStreamOperator.h"
#include "table/utils/VectorBatchSerializationUtils.h"

ChainingOutput::ChainingOutput(Input* op) : operator_(op), announcedStatus(WatermarkStatus::activeStatus)
{
}

ChainingOutput::ChainingOutput(
    Input* op, const std::shared_ptr<omnistream::TaskMetricGroup>& metricGroup, omnistream::OperatorPOD& opConfig)
    : operator_(op),
      announcedStatus(WatermarkStatus::activeStatus)
{
    if (metricGroup != nullptr) {
        auto ptr = metricGroup->GetInternalOperatorIOMetric(opConfig.getName(), "numRecordsIn");
        LOG("numRecordsIn add" << reinterpret_cast<long>(ptr.get()));
        numRecordsIn = std::static_pointer_cast<omnistream::SimpleCounter>(ptr);
        auto bytesPtr = metricGroup->GetInternalOperatorIOMetric(opConfig.getName(), "numBytesIn");
        LOG("numBytesIn add" << reinterpret_cast<long>(bytesPtr.get()));
        numBytesIn = std::static_pointer_cast<omnistream::SimpleCounter>(bytesPtr);
    } else {
        numRecordsIn = nullptr;
        numBytesIn = nullptr;
    }
}

void ChainingOutput::collect(void* record)
{
    LOG(" ChainingOutput collect >>>>>>>>");
    LOG("operator address " + std::to_string(reinterpret_cast<long>(operator_)));
    LOG("pass to operator: " + std::string(operator_->getName()));
    LOG("stream record address  " + std::to_string(reinterpret_cast<long>(record)));
    if (numRecordsIn != nullptr) {
        long numRecordsInCount =
            reinterpret_cast<omnistream::VectorBatch*>(reinterpret_cast<StreamRecord*>(record)->getValue())
                ->GetRowCount();
        numRecordsIn->Inc(numRecordsInCount);
    }
    if (numBytesIn != nullptr) {
        long numBytesInCount = omnistream::VectorBatchSerializationUtils::calculateVectorBatchSerializableSize(
            reinterpret_cast<omnistream::VectorBatch*>(reinterpret_cast<StreamRecord*>(record)->getValue()));
        numBytesIn->Inc(numBytesInCount);
    }
    operator_->processBatch(reinterpret_cast<StreamRecord*>(record));
}

void ChainingOutput::close()
{
    // do nothing
}
void ChainingOutput::emitWatermark(Watermark* mark)
{
    LOG("ChainingOutput::emitWatermark: " << mark->getTimestamp() << " name: " << operator_->getName());
    watermarkGauge.setCurrentwatermark(mark->getTimestamp());
    operator_->ProcessWatermark(mark);
}

void ChainingOutput::emitWatermarkStatus(WatermarkStatus* watermarkStatus)
{
    if (announcedStatus != watermarkStatus->GetStatus()) {
        announcedStatus = watermarkStatus->GetStatus();
        operator_->processWatermarkStatus(watermarkStatus);
    }
}
