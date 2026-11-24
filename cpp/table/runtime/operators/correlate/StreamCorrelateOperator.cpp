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

#include "StreamCorrelateOperator.h"
#include <nlohmann/json.hpp>
#include <algorithm>
#include <cstdlib>

using namespace omniruntime::type;
using namespace omniruntime::vec;
using VarcharVector = Vector<LargeStringContainer<std::string_view>>;

namespace {
constexpr int DEFAULT_MAX_OUTPUT_ROWS_PER_BATCH = 100;
constexpr size_t DEFAULT_MAX_OUTPUT_BYTES_PER_BATCH = 4 * 1024 * 1024;
constexpr const char* MAX_OUTPUT_ROWS_PER_BATCH_ENV = "OMNI_CORRELATE_MAX_OUTPUT_ROWS_PER_BATCH";
constexpr const char* MAX_OUTPUT_BYTES_PER_BATCH_ENV = "OMNI_CORRELATE_MAX_OUTPUT_BYTES_PER_BATCH";
constexpr const char* LOG_INPUT_BATCH_ENV = "OMNI_CORRELATE_LOG_INPUT_BATCH";

int GetMaxOutputRowsPerBatch()
{
    const char* envValue = std::getenv(MAX_OUTPUT_ROWS_PER_BATCH_ENV);
    if (envValue == nullptr || envValue[0] == '\0') {
        return DEFAULT_MAX_OUTPUT_ROWS_PER_BATCH;
    }
    int configuredValue = std::atoi(envValue);
    if (configuredValue <= 0) {
        INFO_RELEASE(
            "Invalid " << MAX_OUTPUT_ROWS_PER_BATCH_ENV << "=" << envValue
                       << ", use default=" << DEFAULT_MAX_OUTPUT_ROWS_PER_BATCH);
        return DEFAULT_MAX_OUTPUT_ROWS_PER_BATCH;
    }
    return configuredValue;
}

size_t GetMaxOutputBytesPerBatch()
{
    const char* envValue = std::getenv(MAX_OUTPUT_BYTES_PER_BATCH_ENV);
    if (envValue == nullptr || envValue[0] == '\0') {
        return DEFAULT_MAX_OUTPUT_BYTES_PER_BATCH;
    }
    long long configuredValue = std::atoll(envValue);
    if (configuredValue <= 0) {
        INFO_RELEASE(
            "Invalid " << MAX_OUTPUT_BYTES_PER_BATCH_ENV << "=" << envValue
                       << ", use default=" << DEFAULT_MAX_OUTPUT_BYTES_PER_BATCH);
        return DEFAULT_MAX_OUTPUT_BYTES_PER_BATCH;
    }
    return static_cast<size_t>(configuredValue);
}

bool ShouldLogInputBatch()
{
    const char* envValue = std::getenv(LOG_INPUT_BATCH_ENV);
    return envValue != nullptr && envValue[0] != '\0' && std::atoi(envValue) != 0;
}

size_t CalculateInputStringBytes(
    omnistream::VectorBatch* inputBatch,
    const std::vector<omniruntime::type::DataTypeId>& inputTypeIds,
    int inputRowCount,
    int& stringColumnCount,
    size_t& maxCellBytes)
{
    size_t totalStringBytes = 0;
    stringColumnCount = 0;
    maxCellBytes = 0;

    int vectorCount = inputBatch->GetVectorCount();
    int typeCount = static_cast<int>(inputTypeIds.size());
    int columnCount = std::min(vectorCount, typeCount);
    for (int col = 0; col < columnCount; col++) {
        auto typeId = inputTypeIds[col];
        if (typeId != omniruntime::type::OMNI_VARCHAR && typeId != omniruntime::type::OMNI_CHAR) {
            continue;
        }
        stringColumnCount++;
        auto* vec = inputBatch->Get(col);
        for (int row = 0; row < inputRowCount; row++) {
            if (vec->IsNull(row)) {
                continue;
            }
            std::string_view value;
            if (vec->GetEncoding() == omniruntime::vec::OMNI_FLAT) {
                value = reinterpret_cast<VarcharVector*>(vec)->GetValue(row);
            } else if (vec->GetEncoding() == omniruntime::vec::OMNI_DICTIONARY) {
                using DictVarcharVec = Vector<DictionaryContainer<std::string_view, LargeStringContainer>>;
                value = reinterpret_cast<DictVarcharVec*>(vec)->GetValue(row);
            } else {
                continue;
            }
            totalStringBytes += value.size();
            maxCellBytes = std::max(maxCellBytes, value.size());
        }
    }

    return totalStringBytes;
}
} // namespace

StreamCorrelateOperator::StreamCorrelateOperator(const nlohmann::json& description, Output* output)
    : description_(description),
      selectedRowsBuffer_(1024),
      executionContext_(std::make_unique<omniruntime::op::ExecutionContext>())
{
    this->setOutput(output);
    parseDescription(description);
    LOG("StreamCorrelateOperator description: " << description.dump());
}

StreamCorrelateOperator::~StreamCorrelateOperator()
{
    clearArgumentExpressions();
    delete timestampedCollector_;
}

void StreamCorrelateOperator::clearArgumentExpressions()
{
    if (argEvaluator_ != nullptr) {
        // ExpressionEvaluator owns and deletes the expression pointers passed to it.
        delete argEvaluator_;
        argEvaluator_ = nullptr;
    } else if (!argExprs_.empty()) {
        // Expressions parsed before the evaluator was created are still ours.
        omniruntime::expressions::Expr::DeleteExprs(argExprs_);
    }
    argExprs_.clear();
}

void StreamCorrelateOperator::open()
{
    clearArgumentExpressions();
    parseDescription(description_);

    // Build expression evaluator only for non-recognized expressions (EVALUATOR mode)
    if (hasFunctionArgs_ && argEvalMode_ == ArgEvalMode::EVALUATOR) {
        INFO_RELEASE(
            "StreamCorrelateOperator::open building expression evaluator, "
            << functionArgsJson_.size() << " expressions, " << argInputTypes_.GetSize() << " input types");
        JSONParser parser = JSONParser();
        for (size_t i = 0; i < functionArgsJson_.size(); i++) {
            INFO_RELEASE(
                "StreamCorrelateOperator::open parsing functionArg[" << i << "]: " << functionArgsJson_[i].dump());
            auto expr = parser.ParseJSON(functionArgsJson_[i]);
            if (expr == nullptr) {
                clearArgumentExpressions();
                THROW_LOGIC_EXCEPTION(
                    "StreamCorrelateOperator: failed to parse functionArgs expression: " + functionArgsJson_[i].dump());
            }
            argExprs_.push_back(expr);
        }
        auto ofConfig = new omniruntime::op::OverflowConfig();
        argEvaluator_ = new omniruntime::codegen::ExpressionEvaluator(argExprs_, argInputTypes_, ofConfig);
        argEvaluator_->ProjectFuncGeneration();
        delete ofConfig; // evaluator copies it internally
        INFO_RELEASE("StreamCorrelateOperator::open expression evaluator built successfully");
    } else if (hasFunctionArgs_) {
        INFO_RELEASE(
            "StreamCorrelateOperator::open using manual evaluation mode="
            << static_cast<int>(argEvalMode_) << " colIndex=" << manualArgColIndex_ << " jsonPath=" << manualJsonPath_);
    }

    delete timestampedCollector_;
    timestampedCollector_ = new TimestampedCollector(this->output);
}

void StreamCorrelateOperator::close()
{
    if (timestampedCollector_) {
        timestampedCollector_->close();
    }
    AbstractStreamOperator<int>::close();
}

void StreamCorrelateOperator::parseDescription(const nlohmann::json& desc)
{
    functionName_ = desc.at("functionName").get<std::string>();
    functionClass_ = desc.at("functionClass").get<std::string>();
    joinType_ = desc.at("joinType").get<std::string>();
    isLeftJoin_ = (joinType_ == "LeftOuterJoin");

    // Parse functionArgs (new path) or functionArgIndices (legacy path)
    if (desc.contains("functionArgs") && desc["functionArgs"].is_array() && !desc["functionArgs"].empty()) {
        hasFunctionArgs_ = true;
        functionArgsJson_ = desc["functionArgs"].get<std::vector<nlohmann::json>>();
    }

    if (desc.contains("functionArgIndices")) {
        functionArgIndices_ = desc.at("functionArgIndices").get<std::vector<int>>();
    }

    inputTypes_ = desc.at("inputTypes").get<std::vector<std::string>>();
    outputTypes_ = desc.at("outputTypes").get<std::vector<std::string>>();
    functionResultTypes_ = desc.at("functionResultTypes").get<std::vector<std::string>>();

    inputColumnCount_ = static_cast<int>(inputTypes_.size());
    outputColumnCount_ = static_cast<int>(outputTypes_.size());

    // 预解析输入列的 OmniTypeId
    inputTypeIds_.clear();
    for (const auto& typeStr : inputTypes_) {
        inputTypeIds_.push_back(LogicalType::flinkTypeToOmniTypeId(typeStr));
    }

    // Detect expression type for manual evaluation (avoids JIT evaluator issues)
    argEvalMode_ = ArgEvalMode::EVALUATOR;
    if (hasFunctionArgs_ && functionArgsJson_.size() == 1) {
        const auto& argJson = functionArgsJson_[0];
        std::string exprType = argJson.value("exprType", "");
        if (exprType == "FIELD_REFERENCE" && argJson.contains("colVal")) {
            argEvalMode_ = ArgEvalMode::FIELD_REF;
            manualArgColIndex_ = argJson["colVal"].get<int>();
        } else if (exprType == "FUNCTION") {
            std::string funcName = argJson.value("function_name", "");
            if (funcName == "json_query" && argJson.contains("arguments") && argJson["arguments"].is_array() &&
                argJson["arguments"].size() == 2) {
                const auto& args = argJson["arguments"];
                if (args[0].value("exprType", "") == "FIELD_REFERENCE" && args[0].contains("colVal") &&
                    args[1].value("exprType", "") == "LITERAL" && args[1].contains("value")) {
                    argEvalMode_ = ArgEvalMode::JSON_QUERY;
                    manualArgColIndex_ = args[0]["colVal"].get<int>();
                    manualJsonPath_ = args[1]["value"].get<std::string>();
                }
            }
        }
    }

    // Build argInputTypes_ for expression evaluator (only if needed)
    if (hasFunctionArgs_ && argEvalMode_ == ArgEvalMode::EVALUATOR) {
        std::vector<omniruntime::type::DataTypePtr> types;
        for (const auto& typeStr : inputTypes_) {
            auto omniType = LogicalType::flinkTypeToOmniTypeId(typeStr);
            types.push_back(std::make_shared<omniruntime::type::DataType>(omniType));
        }
        argInputTypes_ = omniruntime::type::DataTypes(types);
    }

    tableFunction_ = NativeTableFunctionFactory::create(functionName_);
    if (!tableFunction_) {
        THROW_LOGIC_EXCEPTION(
            "Unsupported table function class: " + functionClass_ + ", function name: " + functionName_);
    }

    INFO_RELEASE(
        "StreamCorrelateOperator parsed: functionName="
        << functionName_ << ", joinType=" << joinType_ << ", hasFunctionArgs=" << hasFunctionArgs_
        << ", argEvalMode=" << static_cast<int>(argEvalMode_) << ", manualArgColIndex=" << manualArgColIndex_
        << ", manualJsonPath=" << manualJsonPath_ << ", argIndices=" << functionArgIndices_.size() << ", inputCols="
        << inputColumnCount_ << ", outputCols=" << outputColumnCount_ << ", isLeftJoin=" << isLeftJoin_);
}

// ---------------------------------------------------------------------------
// extractVarchar: Read a varchar string_view from flat or dictionary vector
// ---------------------------------------------------------------------------
std::string_view StreamCorrelateOperator::extractVarchar(omniruntime::vec::BaseVector* vec, int row)
{
    if (vec->GetEncoding() == omniruntime::vec::OMNI_FLAT) {
        return reinterpret_cast<VarcharVector*>(vec)->GetValue(row);
    }
    using DictVarcharVec = Vector<DictionaryContainer<std::string_view, LargeStringContainer>>;
    return reinterpret_cast<DictVarcharVec*>(vec)->GetValue(row);
}

// ---------------------------------------------------------------------------
// evaluateUdtfRows: Evaluate UDTF for each input row, collecting results.
// Handles JSON_QUERY (via operatoromni), FIELD_REF, EVALUATOR, and legacy
// modes. Fills inputRowIndices / udtfResults / hasOutput for the caller.
// ---------------------------------------------------------------------------
void StreamCorrelateOperator::evaluateUdtfRows(
    omnistream::VectorBatch* inputBatch,
    int inputRowCount,
    std::vector<int>& inputRowIndices,
    std::vector<std::string>& udtfResults,
    std::vector<bool>& hasOutput)
{
    omniruntime::vec::VectorBatch* argEvalBatch = nullptr;
    omniruntime::vec::BaseVector* argVec = nullptr;
    int loopCount = inputRowCount;

    if (argEvalMode_ == ArgEvalMode::JSON_QUERY) {
        BaseVector* srcVec = inputBatch->Get(manualArgColIndex_);
        int64_t contextPtr = reinterpret_cast<int64_t>(executionContext_.get());

        for (int row = 0; row < inputRowCount; row++) {
            std::string argValue;

            bool jsonStrIsNull = srcVec->IsNull(row);
            const char* jsonStr = nullptr;
            int32_t jsonStrLen = 0;

            if (!jsonStrIsNull) {
                std::string_view sv = extractVarchar(srcVec, row);
                jsonStr = sv.data();
                jsonStrLen = static_cast<int32_t>(sv.size());
                if (jsonStr == nullptr || jsonStrLen <= 0) {
                    jsonStrIsNull = true;
                }
            }

            bool outIsNull = false;
            int32_t outLen = 0;
            const char* result = omniruntime::codegen::function::JsonQueryRetNull(
                contextPtr,
                jsonStr,
                jsonStrLen,
                jsonStrIsNull,
                manualJsonPath_.c_str(),
                0,
                static_cast<int32_t>(manualJsonPath_.size()),
                false,
                &outIsNull,
                &outLen);

            if (!outIsNull && result != nullptr && outLen > 0) {
                argValue = std::string(result, outLen);
            }

            std::vector<std::string> results = tableFunction_->eval(argValue);
            if (!results.empty()) {
                hasOutput[row] = true;
                for (auto& r : results) {
                    inputRowIndices.push_back(row);
                    udtfResults.push_back(std::move(r));
                }
            }
        }
        executionContext_->GetArena()->Reset();
        return;
    }

    // FIELD_REF, EVALUATOR, or legacy path: resolve argVec
    if (argEvalMode_ == ArgEvalMode::FIELD_REF && manualArgColIndex_ >= 0) {
        argVec = inputBatch->Get(manualArgColIndex_);
    } else if (hasFunctionArgs_ && argEvaluator_ != nullptr) {
        argEvalBatch = argEvaluator_->Evaluate(inputBatch, executionContext_.get(), &selectedRowsBuffer_);
        if (argEvalBatch == nullptr || argEvalBatch->GetVectorCount() == 0) {
            delete argEvalBatch;
            return;
        }
        argVec = argEvalBatch->Get(0);
        if (argVec == nullptr) {
            delete argEvalBatch;
            return;
        }
        loopCount = std::min(inputRowCount, argEvalBatch->GetRowCount());
    } else {
        if (functionArgIndices_.empty()) {
            THROW_LOGIC_EXCEPTION("StreamCorrelateOperator: functionArgIndices is empty and no functionArgs provided");
        }
        argVec = inputBatch->Get(functionArgIndices_[0]);
    }
    for (int row = 0; row < loopCount; row++) {
        std::string argValue;
        if (!argVec->IsNull(row)) {
            std::string_view sv = extractVarchar(argVec, row);
            if (sv.data() != nullptr && sv.size() > 0 && sv.size() < static_cast<size_t>(64 * 1024 * 1024)) {
                argValue = std::string(sv.data(), sv.size());
            } else if (sv.data() != nullptr && sv.size() >= static_cast<size_t>(64 * 1024 * 1024)) {
            }
        }

        std::vector<std::string> results = tableFunction_->eval(argValue);
        if (!results.empty()) {
            hasOutput[row] = true;
            for (auto& r : results) {
                inputRowIndices.push_back(row);
                udtfResults.push_back(std::move(r));
            }
        }
    }
    delete argEvalBatch;
}

omnistream::VectorBatch* StreamCorrelateOperator::buildOutputBatchForRows(
    omnistream::VectorBatch* inputBatch,
    const std::vector<int>& inputRowIndices,
    const std::vector<std::string>* udtfResults)
{
    int totalOutputRows = static_cast<int>(inputRowIndices.size());
    if (totalOutputRows == 0) {
        return nullptr;
    }
    if (udtfResults != nullptr && static_cast<int>(udtfResults->size()) != totalOutputRows) {
        THROW_LOGIC_EXCEPTION("StreamCorrelateOperator: chunk row count does not match UDTF result count");
    }

    auto* outputBatch = new omnistream::VectorBatch(totalOutputRows);

    // Copy input columns by expanded row indices.
    for (int col = 0; col < inputColumnCount_; col++) {
        BaseVector* srcVec = inputBatch->Get(col);
        BaseVector* dstVec;
        if (srcVec->GetTypeId() == OMNI_VARCHAR || srcVec->GetTypeId() == OMNI_CHAR) {
            if (srcVec->GetEncoding() == omniruntime::vec::OMNI_DICTIONARY) {
                dstVec = VectorHelper::CopyPositionsVector(
                    srcVec, const_cast<int*>(inputRowIndices.data()), 0, totalOutputRows);
            } else {
                dstVec = VectorHelper::CreateStringDictionary(
                    inputRowIndices.data(),
                    totalOutputRows,
                    reinterpret_cast<VarcharVector*>(srcVec),
                    srcVec->GetTypeId());
            }
        } else {
            dstVec =
                VectorHelper::CopyPositionsVector(srcVec, const_cast<int*>(inputRowIndices.data()), 0, totalOutputRows);
        }
        outputBatch->Append(dstVec);
    }

    // Append UDTF output columns. For LEFT JOIN padding chunks, udtfResults is null.
    int udtfResultCount = static_cast<int>(functionResultTypes_.size());
    for (int udtfCol = 0; udtfCol < udtfResultCount; udtfCol++) {
        auto* vec = new VarcharVector(totalOutputRows);
        for (int i = 0; i < totalOutputRows; i++) {
            if (udtfResults != nullptr) {
                const std::string& result = (*udtfResults)[i];
                std::string_view sv(result.data(), result.size());
                vec->SetValue(i, sv);
            } else {
                vec->SetNull(i);
            }
        }
        outputBatch->Append(vec);
    }

    auto* oldTimestamps = inputBatch->getTimestamps();
    auto* oldRowKinds = inputBatch->getRowKinds();
    for (int i = 0; i < totalOutputRows; i++) {
        int srcRow = inputRowIndices[i];
        outputBatch->setTimestamp(i, oldTimestamps[srcRow]);
        outputBatch->setRowKind(i, oldRowKinds[srcRow]);
    }

    return outputBatch;
}

// ---------------------------------------------------------------------------
// buildOutputBatch: Construct output VectorBatch from UDTF results.
// Copies input columns by row index, appends UDTF output columns, and handles
// LEFT JOIN null padding for rows with no UDTF output.
// ---------------------------------------------------------------------------
omnistream::VectorBatch* StreamCorrelateOperator::buildOutputBatch(
    omnistream::VectorBatch* inputBatch,
    int inputRowCount,
    const std::vector<int>& inputRowIndices,
    const std::vector<std::string>& udtfResults,
    const std::vector<bool>& hasOutput,
    int outputOffset,
    int outputRowLimit)
{
    // Collect LEFT JOIN null rows
    std::vector<int> leftNullRows;
    if (isLeftJoin_) {
        for (int row = 0; row < inputRowCount; row++) {
            if (!hasOutput[row]) {
                leftNullRows.push_back(row);
            }
        }
    }

    int fullOutputRows = static_cast<int>(inputRowIndices.size()) + static_cast<int>(leftNullRows.size());
    if (fullOutputRows == 0 || outputOffset >= fullOutputRows || outputRowLimit <= 0) {
        return nullptr;
    }
    int totalOutputRows = std::min(outputRowLimit, fullOutputRows - outputOffset);

    // Merge the requested output window: normal expanded rows + LEFT JOIN null rows.
    std::vector<int> allInputRowIndices;
    allInputRowIndices.reserve(totalOutputRows);
    int normalRows = static_cast<int>(inputRowIndices.size());
    for (int i = 0; i < totalOutputRows; i++) {
        int globalRow = outputOffset + i;
        if (globalRow < normalRows) {
            allInputRowIndices.push_back(inputRowIndices[globalRow]);
        } else {
            allInputRowIndices.push_back(leftNullRows[globalRow - normalRows]);
        }
    }

    auto* outputBatch = new omnistream::VectorBatch(totalOutputRows);

    // Copy input columns by expanded row indices
    for (int col = 0; col < inputColumnCount_; col++) {
        BaseVector* srcVec = inputBatch->Get(col);
        BaseVector* dstVec;
        if (srcVec->GetTypeId() == OMNI_VARCHAR || srcVec->GetTypeId() == OMNI_CHAR) {
            if (srcVec->GetEncoding() == omniruntime::vec::OMNI_DICTIONARY) {
                dstVec = VectorHelper::CopyPositionsVector(srcVec, allInputRowIndices.data(), 0, totalOutputRows);
            } else {
                dstVec = VectorHelper::CreateStringDictionary(
                    allInputRowIndices.data(),
                    totalOutputRows,
                    reinterpret_cast<VarcharVector*>(srcVec),
                    srcVec->GetTypeId());
            }
        } else {
            dstVec = VectorHelper::CopyPositionsVector(srcVec, allInputRowIndices.data(), 0, totalOutputRows);
        }
        outputBatch->Append(dstVec);
    }

    // Append UDTF output columns
    int udtfResultCount = static_cast<int>(functionResultTypes_.size());
    for (int udtfCol = 0; udtfCol < udtfResultCount; udtfCol++) {
        auto* vec = new VarcharVector(totalOutputRows);
        for (int i = 0; i < totalOutputRows; i++) {
            int globalRow = outputOffset + i;
            if (globalRow < normalRows) {
                std::string_view sv(udtfResults[globalRow].data(), udtfResults[globalRow].size());
                vec->SetValue(i, sv);
            } else {
                vec->SetNull(i);
            }
        }
        outputBatch->Append(vec);
    }

    // Copy timestamps and RowKinds
    auto* oldTimestamps = inputBatch->getTimestamps();
    auto* oldRowKinds = inputBatch->getRowKinds();
    for (int i = 0; i < totalOutputRows; i++) {
        int srcRow = allInputRowIndices[i];
        outputBatch->setTimestamp(i, oldTimestamps[srcRow]);
        outputBatch->setRowKind(i, oldRowKinds[srcRow]);
    }

    return outputBatch;
}

// ---------------------------------------------------------------------------
// processBatch: Orchestrates UDTF evaluation and output construction.
// ---------------------------------------------------------------------------
void StreamCorrelateOperator::processBatch(StreamRecord* input)
{
    auto* inputBatch = reinterpret_cast<omnistream::VectorBatch*>(input->getValue());

    int inputRowCount = inputBatch->GetRowCount();

    if (ShouldLogInputBatch()) {
        int stringColumnCount = 0;
        size_t maxCellBytes = 0;
        size_t logicalStringBytes =
            CalculateInputStringBytes(inputBatch, inputTypeIds_, inputRowCount, stringColumnCount, maxCellBytes);
        INFO_RELEASE(
            "StreamCorrelateOperator input batch, functionName="
            << functionName_ << ", rows=" << inputRowCount << ", columns=" << inputBatch->GetVectorCount()
            << ", stringColumns=" << stringColumnCount << ", logicalStringBytes=" << logicalStringBytes
            << ", maxCellBytes=" << maxCellBytes << ", vbReportedSize=" << inputBatch->CalculateTotalSize());
    }

    if (inputRowCount == 0) {
        delete inputBatch;
        delete input;
        return;
    }

    int maxOutputRowsPerBatch = GetMaxOutputRowsPerBatch();
    size_t maxOutputBytesPerBatch = GetMaxOutputBytesPerBatch();
    std::vector<int> chunkInputRowIndices;
    std::vector<std::string> chunkUdtfResults;
    chunkInputRowIndices.reserve(maxOutputRowsPerBatch);
    chunkUdtfResults.reserve(maxOutputRowsPerBatch);
    size_t chunkUdtfBytes = 0;
    std::vector<bool> hasOutput(inputRowCount, false);

    auto flushNormalChunk = [&]() {
        if (chunkInputRowIndices.empty()) {
            return;
        }

        auto* outputBatch = buildOutputBatchForRows(inputBatch, chunkInputRowIndices, &chunkUdtfResults);
        if (outputBatch != nullptr) {
            timestampedCollector_->collect(outputBatch);
        }
        chunkInputRowIndices.clear();
        chunkUdtfResults.clear();
        chunkUdtfBytes = 0;
    };

    auto appendUdtfResult = [&](int row, std::string&& result) {
        size_t resultSize = result.size();
        if (!chunkInputRowIndices.empty() && (static_cast<int>(chunkInputRowIndices.size()) >= maxOutputRowsPerBatch ||
                                              chunkUdtfBytes + resultSize > maxOutputBytesPerBatch)) {
            flushNormalChunk();
        }
        hasOutput[row] = true;
        chunkInputRowIndices.push_back(row);
        chunkUdtfResults.push_back(std::move(result));
        chunkUdtfBytes += resultSize;
        if (static_cast<int>(chunkInputRowIndices.size()) >= maxOutputRowsPerBatch ||
            chunkUdtfBytes >= maxOutputBytesPerBatch) {
            flushNormalChunk();
        }
    };

    omniruntime::vec::VectorBatch* argEvalBatch = nullptr;
    omniruntime::vec::BaseVector* argVec = nullptr;
    int loopCount = inputRowCount;

    if (argEvalMode_ == ArgEvalMode::JSON_QUERY) {
        BaseVector* srcVec = inputBatch->Get(manualArgColIndex_);
        int64_t contextPtr = reinterpret_cast<int64_t>(executionContext_.get());

        for (int row = 0; row < inputRowCount; row++) {
            std::string argValue;
            bool jsonStrIsNull = srcVec->IsNull(row);
            const char* jsonStr = nullptr;
            int32_t jsonStrLen = 0;

            if (!jsonStrIsNull) {
                std::string_view sv = extractVarchar(srcVec, row);
                jsonStr = sv.data();
                jsonStrLen = static_cast<int32_t>(sv.size());
                if (jsonStr == nullptr || jsonStrLen <= 0) {
                    jsonStrIsNull = true;
                }
            }

            bool outIsNull = false;
            int32_t outLen = 0;
            const char* result = omniruntime::codegen::function::JsonQueryRetNull(
                contextPtr,
                jsonStr,
                jsonStrLen,
                jsonStrIsNull,
                manualJsonPath_.c_str(),
                0,
                static_cast<int32_t>(manualJsonPath_.size()),
                false,
                &outIsNull,
                &outLen);

            if (!outIsNull && result != nullptr && outLen > 0) {
                argValue = std::string(result, outLen);
            }

            std::vector<std::string> results = tableFunction_->eval(argValue);
            for (auto& r : results) {
                appendUdtfResult(row, std::move(r));
            }
            executionContext_->GetArena()->Reset();
        }

    } else {
        if (argEvalMode_ == ArgEvalMode::FIELD_REF && manualArgColIndex_ >= 0) {
            argVec = inputBatch->Get(manualArgColIndex_);
        } else if (hasFunctionArgs_ && argEvaluator_ != nullptr) {
            argEvalBatch = argEvaluator_->Evaluate(inputBatch, executionContext_.get(), &selectedRowsBuffer_);

            if (argEvalBatch == nullptr || argEvalBatch->GetVectorCount() == 0) {
                delete argEvalBatch;
                delete inputBatch;
                delete input;
                return;
            }
            argVec = argEvalBatch->Get(0);
            if (argVec == nullptr) {
                delete argEvalBatch;
                delete inputBatch;
                delete input;
                return;
            }
            loopCount = std::min(inputRowCount, argEvalBatch->GetRowCount());
        } else {
            if (functionArgIndices_.empty()) {
                THROW_LOGIC_EXCEPTION(
                    "StreamCorrelateOperator: functionArgIndices is empty and no functionArgs provided");
            }
            argVec = inputBatch->Get(functionArgIndices_[0]);
        }

        for (int row = 0; row < loopCount; row++) {
            std::string argValue;
            if (!argVec->IsNull(row)) {
                std::string_view sv = extractVarchar(argVec, row);
                if (sv.data() != nullptr && sv.size() > 0 && sv.size() < static_cast<size_t>(64 * 1024 * 1024)) {
                    argValue = std::string(sv.data(), sv.size());
                }
            }

            std::vector<std::string> results = tableFunction_->eval(argValue);
            for (auto& r : results) {
                appendUdtfResult(row, std::move(r));
            }
        }
    }

    delete argEvalBatch;
    flushNormalChunk();

    if (isLeftJoin_) {
        std::vector<int> leftNullRows;
        leftNullRows.reserve(maxOutputRowsPerBatch);
        auto flushLeftNullChunk = [&]() {
            if (leftNullRows.empty()) {
                return;
            }

            auto* outputBatch = buildOutputBatchForRows(inputBatch, leftNullRows, nullptr);
            if (outputBatch != nullptr) {
                timestampedCollector_->collect(outputBatch);
            }
            leftNullRows.clear();
        };

        for (int row = 0; row < inputRowCount; row++) {
            if (hasOutput[row]) {
                continue;
            }
            leftNullRows.push_back(row);
            if (static_cast<int>(leftNullRows.size()) >= maxOutputRowsPerBatch) {
                flushLeftNullChunk();
            }
        }
        flushLeftNullChunk();
    }

    delete inputBatch;
    delete input;
}
