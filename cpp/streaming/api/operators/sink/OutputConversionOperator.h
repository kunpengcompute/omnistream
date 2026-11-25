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

#ifndef OMNISTREAM_OUTPUTCONVERSIONOPERATOR_H
#define OMNISTREAM_OUTPUTCONVERSIONOPERATOR_H

#include <vector>
#include <memory>
#include <optional>
#include <cstdint>
#include <iostream>
#include <stdexcept>
#include <limits>
#include "streaming/api/operators/OneInputStreamOperator.h"
#include "../TimestampedCollector.h"
#include "table/data/Row.h"

class OutputConversionOperator : public OneInputStreamOperator {
public:
    OutputConversionOperator(const nlohmann::json& config, Output* output);

    void open();

    void initializeState(StreamTaskStateInitializerImpl* initializer, TypeSerializer* keySerializer) override
    {
    }

    void processElement(StreamRecord* record) override;

    void processBatch(StreamRecord* record) override;

    RowData* getEntireRow(omnistream::VectorBatch* batch, int rowId);

    Row* toExternal(RowData* internalRecord);

    Output* getOutput();

    void processWatermark(Watermark* watermark);

    ~OutputConversionOperator() override;

    void processWatermarkStatus(WatermarkStatus* watermarkStatus);

private:
    std::vector<std::string> inputTypes;
    std::vector<std::string> outputTypes;
    Output* output;
    nlohmann::json description;
    TimestampedCollector* collector;
};

#endif // OMNISTREAM_OUTPUTCONVERSIONOPERATOR_H
