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

#include "SinkOperator.h"

void SinkOperator::open()
{
    if (userFunction == nullptr) {
        userFunction = new DiscardingSink(description);
    }
}

const char* SinkOperator::getName()
{
    return "SinkOperator";
}

void SinkOperator::processElement(StreamRecord* record)
{
    LOG("SinkOperator::processElement(StreamRecord *record)");
    userFunction->invoke(record, SinkInputValueType::ROW_DATA);
}

void SinkOperator::processBatch(StreamRecord* record)
{
    LOG("SinkOperator::processBatch(StreamRecord *record)");
    userFunction->invoke(record, SinkInputValueType::VEC_BATCH);
}

std::string SinkOperator::getTypeName()
{
    std::string typeName = "SinkOperator";
    typeName.append(__PRETTY_FUNCTION__);
    return typeName;
}
