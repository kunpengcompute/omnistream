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

#include "OutputConversionOperator.h"

OutputConversionOperator::OutputConversionOperator(const nlohmann::json& config, Output* output)
    : output(output),
      description(config)
{
    this->collector = new TimestampedCollector(this->output);
    inputTypes = config["inputTypes"].get<std::vector<std::basic_string<char>>>();
    outputTypes = config["outputTypes"].get<std::vector<std::basic_string<char>>>();
}

void OutputConversionOperator::open()
{
}

Row* OutputConversionOperator::toExternal(RowData* internalRecord)
{
    std::vector<std::any> fieldByPosition;
    std::map<std::string, std::any> fieldByName;
    std::map<std::string, int> positionByName;
    for (size_t i = 0; i < outputTypes.size(); i++) {
        if (outputTypes[i] == "BIGINT" || outputTypes[i].find("TIMESTAMP") != std::string::npos) {
            fieldByPosition.emplace_back(*internalRecord->getLong(i));
        } else if (outputTypes[i] == "INTEGER") {
            fieldByPosition.emplace_back(*internalRecord->getInt(i));
        } else if (outputTypes[i] == "DOUBLE") {
            fieldByPosition.emplace_back(*internalRecord->getLong(i));
        } else if (outputTypes[i] == "BOOLEAN") {
            fieldByPosition.emplace_back(*internalRecord->getInt(i));
        } else if (
            outputTypes[i] == "STRING" || outputTypes[i] == "VARCHAR" || outputTypes[i] == "VARCHAR(2147483647)") {
            BinaryStringData* stringRowData = internalRecord->getString(i);
            std::string strValue = std::string(stringRowData->toString()->begin(), stringRowData->toString()->end());
            fieldByPosition.emplace_back(strValue);
        } else {
            LOG("Data type not supported: " << outputTypes[i]);
            throw std::runtime_error("Data type not supported");
        }
    }
    auto row = new Row(internalRecord->getRowKind(), fieldByPosition, fieldByName, positionByName);
    return row;
}

RowData* OutputConversionOperator::getEntireRow(omnistream::VectorBatch* batch, int rowId)
{
    int colsCount = batch->GetVectorCount();
    BinaryRowData* row = BinaryRowData::createBinaryRowDataWithMem(colsCount);
    for (int32_t i = 0; i < colsCount; i++) {
        int pos = i;
        if (inputTypes[i] == "BIGINT" || inputTypes[i].find("TIMESTAMP") != std::string::npos) {
            row->setLong(i, reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(batch->Get(pos))->GetValue(rowId));
        } else if (inputTypes[i] == "INTEGER") {
            row->setInt(i, reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(batch->Get(pos))->GetValue(rowId));
        } else if (inputTypes[i] == "DOUBLE") {
            row->setLong(i, reinterpret_cast<omniruntime::vec::Vector<double>*>(batch->Get(pos))->GetValue(rowId));
        } else if (inputTypes[i] == "BOOLEAN") {
            row->setInt(i, reinterpret_cast<omniruntime::vec::Vector<bool>*>(batch->Get(pos))->GetValue(rowId));
        } else if (inputTypes[i] == "STRING" || inputTypes[i] == "VARCHAR" || inputTypes[i] == "VARCHAR(2147483647)") {
            auto str =
                reinterpret_cast<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(
                    batch->Get(pos))
                    ->GetValue(rowId);
            auto u32Str = std::make_unique<std::u32string>(str.begin(), str.end());
            row->setString(i, new BinaryStringData(u32Str.get()));
        } else {
            LOG("Data type not supported: " << inputTypes[i]);
            delete row;
            throw std::runtime_error("Data type not supported");
        };
    }
    row->setRowKind(batch->getRowKind(rowId));
    return row;
}

void OutputConversionOperator::processBatch(StreamRecord* record)
{
    Row* externalRecord;
    omnistream::VectorBatch* input = reinterpret_cast<omnistream::VectorBatch*>(record->getValue());
    int rowCount = input->GetRowCount();
    for (int row = 0; row < rowCount; ++row) {
        RowData* currentRow = getEntireRow(input, row);
        externalRecord = toExternal(currentRow);
        externalRecord->setInternalRow(currentRow);
        collector->collectExternalRow(externalRecord);
        delete externalRecord;
    }
}

void OutputConversionOperator::processElement(StreamRecord* record)
{
}

Output* OutputConversionOperator::getOutput()
{
    return this->output;
}

void OutputConversionOperator::processWatermark(Watermark* watermark)
{
}

void OutputConversionOperator::processWatermarkStatus(WatermarkStatus* watermarkStatus)
{
}

OutputConversionOperator::~OutputConversionOperator()
{
    delete collector;
}
