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

#include "StreamExpand.h"
#include "StreamCalc.h"
#include <utility>

StreamExpand::StreamExpand(const nlohmann::json& description, Output* output)
    : description_(description),
      executionContext(std::make_unique<omniruntime::op::ExecutionContext>()),
      selectedRowsBuffer(1024)
{
    this->setOutput(output);
    LOG("StreamExpand description: " << description);
}

StreamExpand::~StreamExpand()
{
    delete timestampedCollector_;
    timestampedCollector_ = nullptr;
}

void StreamExpand::parseDescription(nlohmann::json& subDesc, int index)
{
    std::vector<std::string> inputTypeStrs = subDesc["inputTypes"].get<std::vector<std::string>>();
    std::vector<omniruntime::type::DataTypePtr> types;
    for (std::string otype : inputTypeStrs) {
        auto omniType = LogicalType::flinkTypeToOmniTypeId(otype);
        types.push_back(std::make_shared<omniruntime::type::DataType>(omniType));
    }
    inputTypes[index] = omniruntime::type::DataTypes(types);
}

void StreamExpand::open()
{
    INFO_RELEASE("StreamExpand descriptionJson: " << description_.dump());
    if (description_.contains("projects")) {
        std::vector<nlohmann::json> projections = description_["projects"].get<std::vector<nlohmann::json>>();
        int len = projections.size();
        inputTypes.resize(len);
        exprEvaluators.resize(len);
        projExprs.resize(len);
        for (int i = 0; i < projections.size(); i++) {
            JSONParser parser = JSONParser();
            nlohmann::json subDesc = projections[i];
            parseDescription(subDesc, i);
            if (subDesc.contains("indices")) {
                for (auto& index : subDesc["indices"]) {
                    auto expr = parser.ParseJSON(index);
                    if (expr == nullptr) {
                        omniruntime::expressions::Expr::DeleteExprs(projExprs[i]);
                        projExprs[i].clear();
                        throw std::runtime_error(
                            "Unsupported StreamExpand expression for native codegen: " + index.dump());
                    }
                    projExprs[i].push_back(expr);
                }
            }
            auto ofConfig = new omniruntime::op::OverflowConfig();
            INFO_RELEASE("projExprs[i] size " << projExprs[i].size());
            omniruntime::codegen::ExpressionEvaluator* exprEvaluator =
                new omniruntime::codegen::ExpressionEvaluator(projExprs[i], inputTypes[i], ofConfig);
            exprEvaluator->ProjectFuncGeneration();
            exprEvaluators[i] = exprEvaluator;
        }
    }
    timestampedCollector_ = new TimestampedCollector(this->output);
}

void StreamExpand::close()
{
    AbstractStreamOperator<int>::close();
}

omnistream::VectorBatch* StreamExpand::copyTimestampAndKind(
    omnistream::VectorBatch* srcVb, std::unique_ptr<omniruntime::vec::VectorBatch> projectedVecs)
{
    int32_t rowCnt = srcVb->GetRowCount();
    if (rowCnt <= 0) {
        return nullptr;
    }
    int64_t* target_timestamps = new int64_t[rowCnt];
    RowKind* target_rowKinds = new RowKind[rowCnt];
    int64_t* srcTimestamps = srcVb->getTimestamps();
    RowKind* srcRowKinds = srcVb->getRowKinds();
    memcpy_s(target_timestamps, sizeof(int64_t) * rowCnt, srcTimestamps, sizeof(int64_t) * rowCnt);
    memcpy_s(target_rowKinds, sizeof(RowKind) * rowCnt, srcRowKinds, sizeof(RowKind) * rowCnt);
    auto outputBatch = new omnistream::VectorBatch(std::move(projectedVecs), target_timestamps, target_rowKinds);
    return outputBatch;
}

void StreamExpand::processBatch(StreamRecord* input)
{
    auto record = reinterpret_cast<omnistream::VectorBatch*>(input->getValue());
    for (auto expr : exprEvaluators) {
        auto projectedVecs = std::unique_ptr<omniruntime::vec::VectorBatch>(
            expr->Evaluate(record, executionContext.get(), &selectedRowsBuffer));
        auto outputBatch = copyTimestampAndKind(record, std::move(projectedVecs));
        if (outputBatch) {
            timestampedCollector_->collect(outputBatch);
        }
    }
    delete record;
}

const char* StreamExpand::getName()
{
    return OneInputStreamOperator::getName();
}
