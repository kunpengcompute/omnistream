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

#include "TimeStampInserterSinkOperator.h"

void TimeStampInserterSinkOperator::open()
{
    if (userFunction == nullptr) {
        userFunction = new DiscardingSink(description);
    }
}

const char* TimeStampInserterSinkOperator::getName()
{
    return "TimeStampInserterSinkOperator";
}

void TimeStampInserterSinkOperator::processElement(StreamRecord* record)
{
    LOG("SinkOperator::processElement(StreamRecord *record)");
    userFunction->invoke(record, SinkInputValueType::ROW_DATA);
}

void TimeStampInserterSinkOperator::processBatch(StreamRecord* record)
{
    LOG("SinkOperator::processBatch(StreamRecord *record)");
    auto vb = reinterpret_cast<omnistream::VectorBatch*>(record->getValue());
    if (vb != nullptr) {
        LOG("set time");
        int rowCount = vb->GetRowCount();
        for (int i = 0; i < rowCount; i++) {
            vb->setTimestamp(i, vb->GetValueAt<int64_t>(rowTimeIndex, i));
        }
    }
    LOG("finish set time");

    this->output->collect(record);
}

std::string TimeStampInserterSinkOperator::getTypeName()
{
    std::string typeName = "TimeStampInserterSinkOperator";
    typeName.append(__PRETTY_FUNCTION__);
    return typeName;
}
