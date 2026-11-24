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

//
// created for projection operation.
//

#ifndef FLINK_TNEL_STREAMCALC_H
#define FLINK_TNEL_STREAMCALC_H

#include <functional>
#include <iostream>
#include <nlohmann/json.hpp>
#include <variant>
#include <vector>
#include "OmniOperatorJIT/core/src/vector/vector_batch.h"
#include "OmniOperatorJIT/core/src/expression/expressions.h"
#include "OmniOperatorJIT/core/src/expression/expr_printer.h"
#include "OmniOperatorJIT/core/src/expression/jsonparser/jsonparser.h"
#include "Output.h"
#include "AbstractUdfStreamOperator.h"
#include "OneInputStreamOperator.h"
#include "TimestampedCollector.h"
#include "table/data/GenericRowData.h"
#include "table/data/TimestampData.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/data/binary/BinaryStringData.h"

using namespace omniruntime::expressions;
using ProjectFunc = int32_t (*)(const int64_t*, const uint8_t*, int32_t*, int64_t*, uint8_t*, int32_t*, int64_t);
class StreamCalc : public OneInputStreamOperator,
                   public AbstractStreamOperator<RowData*> { // StreamCalc is not stateful, this RowData* is not needed
public:
    explicit StreamCalc(const nlohmann::json& description, Output* output);
    ~StreamCalc() override;
    void processElement(StreamRecord* record) override;
    void open() override;
    void close() override;
    StreamRecord* getRecord()
    {
        return reUsableRecord_;
    }
    const char* getName() override;

    void initializeState(StreamTaskStateInitializerImpl* initializer, TypeSerializer* keySerializer) override
    {
        LOG("StreamCalc initializeState()");
        // initializeState takes ownership of keySerializer: stateful operators hand it to
        // AbstractKeyedStateBackend, whose destructor frees it. This operator is stateless and
        // has no backend to hand it to, so it frees it here rather than dropping it. The chain
        // allocates a fresh one per operator, so this is never shared. Null for non-SQL types.
        delete keySerializer;
    }

    std::string getTypeName() override
    {
        std::string typeName = "StreamCalc";
        typeName.append(__PRETTY_FUNCTION__);
        return typeName;
    }

    void processWatermarkStatus(WatermarkStatus* watermarkStatus) override
    {
        output->emitWatermarkStatus(watermarkStatus);
    }

private:
    void parseDescription(const nlohmann::json& description);
    using ProjFuncType = void (*)(RowData*, int, RowData*, int);
    int extractPrecision(std::basic_string<char>& basicString);
    void GetMsgFromJson();
    void ParseProjExprs(JSONParser* parser);
    std::vector<ProjFuncType> projFuncs_;
    std::vector<int32_t> outputTypeIds_;
    // std::vector<int32_t> inputTypeIds_;
    std::vector<int32_t> outputLengths_;
    std::vector<int32_t> inputLengths_;
    std::vector<int> outputIndexes_;
    int outputSize_;
    int inputSize_;
    nlohmann::json description_;
    bool isSimpleProjection_ = false;
    bool hasFilter = false;
    TimestampedCollector* timestampedCollector_;
    BinaryRowData* reUsableBinaryRow_;
    StreamRecord* reUsableRecord_;

    const static int SEG_SIZE = 2048;
    // MemorySegment ** backData_;
    // int numSegment_;
    std::vector<Expr*> projExprs;
    Expr* filterCondition = nullptr;
    ProjectFunc projector;
};

#endif
