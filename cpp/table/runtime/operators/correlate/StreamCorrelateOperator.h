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

#ifndef OMNISTREAM_STREAMCORRELATEOPERATOR_H
#define OMNISTREAM_STREAMCORRELATEOPERATOR_H

#include <functional>
#include <iostream>
#include <nlohmann/json.hpp>
#include <vector>
#include <string>

#include "streaming/api/operators/Output.h"
#include "streaming/api/operators/OneInputStreamOperator.h"
#include "streaming/api/operators/AbstractStreamOperator.h"
#include "streaming/api/operators/TimestampedCollector.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "table/types/logical/LogicalType.h"
#include "core/include/common.h"
#include "table/runtime/generated/function/tablefunction/NativeTableFunctionFactory.h"

#include "OmniOperatorJIT/core/src/type/data_types.h"
#include "OmniOperatorJIT/core/src/expression/expressions.h"
#include "OmniOperatorJIT/core/src/expression/jsonparser/jsonparser.h"
#include "OmniOperatorJIT/core/src/codegen/expr_evaluator.h"
#include "OmniOperatorJIT/core/src/operator/execution_context.h"
#include "OmniOperatorJIT/core/src/operator/config/operator_config.h"
#include "OmniOperatorJIT/core/src/memory/aligned_buffer.h"
#include "OmniOperatorJIT/core/src/codegen/functions/stringfunctions.h"

class StreamCorrelateOperator : public OneInputStreamOperator, public AbstractStreamOperator<int> {
public:
    explicit StreamCorrelateOperator(const nlohmann::json& description, Output* output);
    ~StreamCorrelateOperator() override;

    void processBatch(StreamRecord* input) override;

    void processElement(StreamRecord* record) override
    {
        NOT_IMPL_EXCEPTION;
    }

    void open() override;
    void close() override;

    const char* getName() override
    {
        return "StreamCorrelateOperator";
    }

    void initializeState(StreamTaskStateInitializerImpl* initializer, TypeSerializer* keySerializer) override
    {
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
        return "StreamCorrelateOperator";
    }

    std::shared_ptr<omnistream::TaskMetricGroup> GetMectrics() override
    {
        return AbstractStreamOperator<int>::GetMectrics();
    }

private:
    void parseDescription(const nlohmann::json& desc);
    void clearArgumentExpressions();

    // Extract a varchar string_view from a flat or dictionary-encoded vector
    static std::string_view extractVarchar(omniruntime::vec::BaseVector* vec, int row);

    // Evaluate UDTF for all input rows, collecting per-row results
    void evaluateUdtfRows(
        omnistream::VectorBatch* inputBatch,
        int inputRowCount,
        std::vector<int>& inputRowIndices,
        std::vector<std::string>& udtfResults,
        std::vector<bool>& hasOutput);

    // Build output VectorBatch from UDTF results and LEFT JOIN null padding
    omnistream::VectorBatch* buildOutputBatch(
        omnistream::VectorBatch* inputBatch,
        int inputRowCount,
        const std::vector<int>& inputRowIndices,
        const std::vector<std::string>& udtfResults,
        const std::vector<bool>& hasOutput,
        int outputOffset,
        int outputRowLimit);

    // Build output VectorBatch from one already-split chunk. If udtfResults is
    // null, UDTF output columns are emitted as nulls for LEFT JOIN padding rows.
    omnistream::VectorBatch* buildOutputBatchForRows(
        omnistream::VectorBatch* inputBatch,
        const std::vector<int>& inputRowIndices,
        const std::vector<std::string>* udtfResults);

    // JsonSplit 的 native 实现：解析 JSON 数组字符串，返回各元素
    std::vector<std::string> evalJsonSplit(const std::string& input);

    nlohmann::json description_;
    TimestampedCollector* timestampedCollector_ = nullptr;

    // 从 description 解析出的元信息
    std::unique_ptr<NativeTableFunction> tableFunction_;
    std::string functionName_;
    std::string functionClass_;
    std::string joinType_;                // "InnerJoin" or "LeftOuterJoin"
    std::vector<int> functionArgIndices_; // UDTF 参数对应的输入列索引
    std::vector<std::string> inputTypes_;
    std::vector<std::string> outputTypes_;
    std::vector<std::string> functionResultTypes_;
    int inputColumnCount_ = 0;
    int outputColumnCount_ = 0;
    bool isLeftJoin_ = false;

    // 输入列的 OmniTypeId（用于按行索引复制列）
    std::vector<omniruntime::type::DataTypeId> inputTypeIds_;

    // Expression-based argument evaluation (for nested expressions like JSON_QUERY)
    bool hasFunctionArgs_ = false;
    std::vector<nlohmann::json> functionArgsJson_;
    std::vector<omniruntime::expressions::Expr*> argExprs_;
    omniruntime::codegen::ExpressionEvaluator* argEvaluator_ = nullptr;
    omniruntime::type::DataTypes argInputTypes_;
    std::unique_ptr<omniruntime::op::ExecutionContext> executionContext_;
    omniruntime::mem::AlignedBuffer<int32_t> selectedRowsBuffer_;

    // Manual evaluation for recognized expressions (avoids JIT evaluator issues)
    enum class ArgEvalMode {
        EVALUATOR,
        FIELD_REF,
        JSON_QUERY
    };
    ArgEvalMode argEvalMode_ = ArgEvalMode::EVALUATOR;
    int manualArgColIndex_ = -1;
    std::string manualJsonPath_;
};

#endif // OMNISTREAM_STREAMCORRELATEOPERATOR_H
