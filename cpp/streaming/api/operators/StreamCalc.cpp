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

#include <regex>
#include <nlohmann/json.hpp>
#include "memory/MemorySegmentFactory.h"
#include "table/types/logical/LogicalType.h"
#include "StreamCalc.h"

using namespace omniruntime::type;

using json = nlohmann::json;
bool checkAllFieldReference(const json& jsonDesc)
{
    // check all the field references
    if (jsonDesc.contains("indices") && jsonDesc["indices"].is_array()) {
        for (auto& index : jsonDesc["indices"]) {
            if (index["exprType"] != "FIELD_REFERENCE") {
                return false;
            }
        }
        return true;
    }
    return false;
}

StreamCalc::StreamCalc(const nlohmann::json& description, Output* output) : description_(description)
{
    this->setOutput(output);
    reUsableBinaryRow_ = nullptr;
    reUsableRecord_ = new StreamRecord();
    LOG("StreamCalc description: " << description);
    parseDescription(description);

    inputLengths_.resize(inputSize_, 1);
    outputLengths_.resize(outputSize_, 1);

    auto* bytes = new uint8_t[SEG_SIZE];

    LOG("outputSize_.size: " << outputSize_);
    reUsableBinaryRow_ = new BinaryRowData(outputSize_);
    int length = BinaryRowData::calculateFixPartSizeInBytes(outputSize_);
    reUsableBinaryRow_->pointTo(bytes, 0, length, SEG_SIZE);
}

StreamCalc::~StreamCalc()
{
    delete timestampedCollector_;
    delete[] reUsableBinaryRow_->getSegment();
    delete reUsableBinaryRow_;
    delete reUsableRecord_;
}

void StreamCalc::processElement(StreamRecord* record)
{
    LOG("===>>>>>>");
    auto* row = static_cast<RowData*>(record->getValue());
    reUsableRecord_->setTag(record->getTag());
    reUsableRecord_->setTimestamp(record->getTimestamp());
    reUsableBinaryRow_->setRowKind((reinterpret_cast<RowData*>(record->getValue())->getRowKind()));
    reUsableBinaryRow_->setSizeInBytes(
        BinaryRowData::calculateFixPartSizeInBytes(
            outputSize_)); // reset to the default size, updated later for variable length fields
    int32_t result = 0;

    if (isSimpleProjection_ && (!hasFilter)) {
        for (size_t i = 0; i < outputIndexes_.size(); i++) {
            ProjFuncType projFunc = projFuncs_[i];
            projFunc(row, outputIndexes_[i], reUsableBinaryRow_, i);
        }
    } else {
        LOG("codegen starts");
        auto* binaryRow = static_cast<BinaryRowData*>(record->getValue());
        auto inputAddr = binaryRow->getSegment();
        auto outputAddr = reUsableBinaryRow_->getSegment();

        int nullbitsSize = binaryRow->getNullBitsSizeInBytes();
        result = this->projector(
            reinterpret_cast<int64_t*>(inputAddr + nullbitsSize),
            inputAddr,
            inputLengths_.data(),
            reinterpret_cast<int64_t*>(outputAddr + nullbitsSize),
            outputAddr,
            outputLengths_.data(),
            123123);
    }
    if ((isSimpleProjection_ && (!hasFilter)) || result == 1) {
        timestampedCollector_->setTimestamp(reUsableRecord_);
        timestampedCollector_->collect(reUsableBinaryRow_);
    }
}

void StreamCalc::open()
{
    LOG("===>>>>>>");
    if (isSimpleProjection_ && (!hasFilter)) {
        for (size_t i = 0; i < outputTypeIds_.size(); i++) {
            switch (outputTypeIds_[i]) {
                case DataTypeId::OMNI_INT:
                    projFuncs_.push_back([](RowData* from, int fromIndex, RowData* to, int toIndex) {
                        to->setInt(toIndex, *from->getInt(fromIndex));
                    });
                    break;
                case DataTypeId::OMNI_LONG:
                    projFuncs_.push_back([](RowData* from, int fromIndex, RowData* to, int toIndex) {
                        to->setLong(toIndex, *from->getLong(fromIndex));
                    });
                    break;
                case DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                case DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE: {
                    std::vector<std::string> outputTypes = description_["outputTypes"].get<std::vector<std::string>>();
                    if (extractPrecision(outputTypes[i]) > 3) {
                        projFuncs_.push_back([](RowData* from, int fromIndex, RowData* to, int toIndex) {
                            to->setTimestamp(toIndex, from->getTimestampPrecise(fromIndex), 4);
                        });
                    } else {
                        projFuncs_.push_back([](RowData* from, int fromIndex, RowData* to, int toIndex) {
                            to->setTimestamp(toIndex, from->getTimestamp(fromIndex), 3);
                        });
                    }
                    break;
                }
                case DataTypeId::OMNI_VARCHAR:
                    projFuncs_.push_back([](RowData* from, int fromIndex, RowData* to, int toIndex) {
                        to->setString(toIndex, from->getString(fromIndex));
                    });
                    break;
                default:
                    THROW_LOGIC_EXCEPTION(
                        "Type ID '" + std::to_string(static_cast<int>(outputTypeIds_[i])) + "' not supported");
            }
        }
    } else {
        GetMsgFromJson();
    }
    timestampedCollector_ = new TimestampedCollector(this->output);
}

void StreamCalc::close()
{
    AbstractStreamOperator<RowData*>::close();
}

const char* StreamCalc::getName()
{
    // later, should use its unique name
    return "StreamCalc";
}

void StreamCalc::parseDescription(const json& descriptionJson)
{
    std::vector<std::string> outputTypes = descriptionJson["outputTypes"].get<std::vector<std::string>>();
    outputSize_ = outputTypes.size();
    for (std::string type : outputTypes) {
        outputTypeIds_.push_back(LogicalType::flinkTypeToOmniTypeId(type));
    }
    std::vector<std::string> inputTypes = descriptionJson["inputTypes"].get<std::vector<std::string>>();
    inputSize_ = inputTypes.size();

    if (descriptionJson.contains("condition") && (!descriptionJson["condition"].is_null())) {
        hasFilter = true;
    }
    isSimpleProjection_ = checkAllFieldReference(descriptionJson);
    if (isSimpleProjection_) {
        for (auto& index : descriptionJson["indices"]) {
            outputIndexes_.push_back(index["colVal"]);
        }
    } else {
        LOG("Mapping is not supported yet.");
    }
}

int StreamCalc::extractPrecision(std::basic_string<char>& inputTypeString)
{
    // Regular expression to extract the number inside parentheses
    std::regex pattern("\\((\\d+)\\)");
    std::smatch match;
    if (std::regex_search(inputTypeString, match, pattern)) {
        std::string number = match[1]; // The first capture group contains the number
    }
    return 0;
}

void StreamCalc::GetMsgFromJson()
{
    JSONParser* parser = new JSONParser();
    if (description_.contains("indices")) {
        ParseProjExprs(parser);
    }
    if (hasFilter) {
        filterCondition = parser->ParseJSON(description_["condition"]);
    }
    // todo: I removed the row-based codegen StreamCalc won't work anymore
    projector = nullptr;
}

void StreamCalc::ParseProjExprs(JSONParser* parser)
{
    for (auto& index : description_["indices"]) {
        if (index.contains("operator") && !index["operator"].is_null() &&
            (index["operator"].get<std::string>() == "CAST")) {
            // Currently, we just pass the values in CAST expr, there is no real "CAST" operation.
            LOG("Warning: there is a CAST operation.");
            if (!index.contains("expr") || index["expr"].is_null()) {
                LOG("Error: there is no expr to CAST");
            }
            Expr* expr = parser->ParseJSON(index["expr"]);
            projExprs.push_back(expr);
        } else {
            Expr* expr = parser->ParseJSON(index);
            projExprs.push_back(expr);
        }
    }
}
