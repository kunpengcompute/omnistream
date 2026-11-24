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

#ifndef FLINK_TNEL_CSVTABLESOURCE_H
#define FLINK_TNEL_CSVTABLESOURCE_H

#include <string>
#include <vector>
#include <cassert>
#include <fstream>
#include <emhash7.hpp>
#include <nlohmann/json.hpp>
#include "core/typeinfo/TypeInformation.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "OmniOperatorJIT/core/src/type/data_type.h"
#include "functions/Collector.h"

class CsvTableSource {
public:
    CsvTableSource(std::string filepath, std::vector<std::string> fieldTypeStrs)
        : fieldTypeStrs(fieldTypeStrs),
          filepath(filepath)
    {
    }
    size_t countCsvRows();
    std::string getFilePath() const
    {
        return filepath;
    }
    std::vector<std::string> getTableFieldTypes() const
    {
        return fieldTypeStrs;
    }

private:
    std::vector<std::string> fieldTypeStrs;
    std::string filepath;
};

template <typename T>
inline void CsvStrConverterFunc(const std::string& inStr, omniruntime::vec::BaseVector* vec, int rowIndex)
{
    // todo: implement it for all dataTypes
    if constexpr (std::is_same_v<int64_t, T>) {
        static_cast<omniruntime::vec::Vector<int64_t>*>(vec)->SetValue(rowIndex, std::stol(inStr));
    } else if constexpr (std::is_same_v<int32_t, T>) {
        static_cast<omniruntime::vec::Vector<int32_t>*>(vec)->SetValue(rowIndex, std::stoi(inStr));
    } else if constexpr (std::is_same_v<std::string_view, T>) {
        std::string_view inStrView(inStr.data(), inStr.size());
        using VarcharVecType = omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>;
        static_cast<VarcharVecType*>(vec)->SetValue(rowIndex, inStrView);
    }
}

template <typename K>
class CsvLookupFunction {
public:
    ~CsvLookupFunction()
    {
        delete csvDataVecBatch;
    }

    // The lookup op description
    CsvLookupFunction(const nlohmann::json& description, CsvTableSource* src) : description(description), src(src)
    {
        // prepare the csv side vectorbatch
        int hashRowCnt = src->countCsvRows();
        csvDataVecBatch = new omniruntime::vec::VectorBatch(hashRowCnt);
        // Used for read csv file into vectorBatch
        auto lookupTypeStrs = description["lookupInputTypes"].get<std::vector<std::string>>();
        for (size_t i = 0; i < lookupTypeStrs.size(); i++) {
            switch (LogicalType::flinkTypeToOmniTypeId(lookupTypeStrs[i])) {
                case omniruntime::type::OMNI_LONG: {
                    csvStrConverters.push_back(CsvStrConverterFunc<int64_t>);
                    auto vec = new omniruntime::vec::Vector<int64_t>(hashRowCnt);
                    csvDataVecBatch->Append(vec);
                    break;
                }
                case omniruntime::type::OMNI_CHAR:
                case omniruntime::type::OMNI_VARCHAR: {
                    csvStrConverters.push_back(CsvStrConverterFunc<std::string_view>);
                    using VarcharVecType =
                        omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>;
                    auto vec = new VarcharVecType(hashRowCnt);
                    csvDataVecBatch->Append(vec);
                    break;
                }
                case omniruntime::type::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
                case omniruntime::type::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                case omniruntime::type::OMNI_TIMESTAMP: {
                    csvStrConverters.push_back(CsvStrConverterFunc<int64_t>);
                    auto vec = new omniruntime::vec::Vector<int64_t>(hashRowCnt);
                    csvDataVecBatch->Append(vec);
                    break;
                }
                default: std::runtime_error("not supported type");
            }
        }
        // At this point, the CsvDataVecBatch is still empty
    }

    // Construct the hash table from csv file
    void open()
    {
        auto lookupKeys = description["lookupKeys"];
        auto selectedFields = description["selectedFields"].get<std::vector<int>>();
        for (const auto& [key, value] : lookupKeys.items()) {
            // Key from the input side
            sourceKeys.push_back(value["index"]);
            int targetIdx = std::stoi(key);
            if (targetIdx == -1) {
                throw std::runtime_error("CsvLookupFunction open: targetIdx is -1");
            }

            // Key from filesystem side
            targetKeys.push_back(targetIdx);
        }
        if (sourceKeys.size() != 1 || targetKeys.size() != 1) {
            NOT_IMPL_EXCEPTION;
        }

        int32_t hashKeyIndex = targetKeys[0];
        if (src->getTableFieldTypes()[hashKeyIndex] != "BIGINT" &&
            src->getTableFieldTypes()[hashKeyIndex] != "BIGINT NOT NULL") {
            NOT_IMPL_EXCEPTION;
        }

        // Read line by line and construct the dataMap
        std::ifstream file(src->getFilePath());
        int32_t irow = 0;
        std::string line;
        std::string keyStr;
        auto hashVectors = csvDataVecBatch->GetVectors();
        while (std::getline(file, line)) {
            std::stringstream ss(line);
            std::string token;
            int icol = 0;
            size_t colIndex = 0;
            // get the i-th token from a row
            while (std::getline(ss, token, ',') && colIndex < selectedFields.size()) {
                if (icol == selectedFields[colIndex]) {
                    csvStrConverters[colIndex](token, hashVectors[colIndex], irow);
                    colIndex++;
                }
                if (icol == hashKeyIndex) {
                    keyStr = token;
                }
                icol++;
            }
            // todo: here needs to convert key based on type
            K key = std::stol(keyStr);
            // insert this row to map
            dataMap[key].push_back(irow);
            irow++;
        }
        file.close();
    }

    // Used for testing
    std::vector<int32_t>& getTestFunc(K key)
    {
        return dataMap[key];
    }

    // Search for matched rows and convert these rows to a vector batch
    // Here collector is the TableFunctionCollector from FlatMapFunction
    void eval(omnistream::VectorBatch* vb, Collector* collector)
    {
        int32_t totRowCnt = 0;
        // vb'w rowId and hashside's rowIds
        auto probekeyCol = reinterpret_cast<omniruntime::vec::Vector<K>*>(vb->GetVectors()[sourceKeys[0]]);
        // Get matched rows and build output as a vector batch
        int32_t rowCount = vb->GetRowCount();
        // i-th row from probe is matched with a vector of csv rows
        std::vector<std::tuple<int32_t*, int32_t>> matchedRows(rowCount, {nullptr, 0});
        for (int i = 0; i < rowCount; i++) {
            auto key = probekeyCol->GetValue(i);
            auto it = dataMap.find(key);
            if (it != dataMap.end()) {
                matchedRows[i] = {it->second.data(), it->second.size()};
                totRowCnt += it->second.size();
            }
        }
        auto output = buildOutput(vb, matchedRows, totRowCnt);
        collector->collect(output);
        delete vb;
    }

private:
    omnistream::VectorBatch* buildOutput(
        omnistream::VectorBatch* vb, std::vector<std::tuple<int32_t*, int32_t>>& matchedRows, int32_t totRowCnt)
    {
        // In matchedRows, it stores {probe_row_index : {vector of matched csv row indices}}
        auto output = new omnistream::VectorBatch(totRowCnt);
        // build the probe side, it is written as the left side in output
        omniruntime::vec::BaseVector* outCol = nullptr;
        BuildProbeOutput(vb, matchedRows, totRowCnt, output, outCol);
        // build the hash side
        BuildHashOutput(vb, matchedRows, totRowCnt, output, outCol);

        int32_t outputRowIndex = 0;
        int32_t rowIndex = 0;
        for (size_t probeRow = 0; probeRow < matchedRows.size(); probeRow++) {
            int32_t targetRowsCnt = std::get<1>(matchedRows[probeRow]);
            if (targetRowsCnt == 0) {
                continue; // no match found for this one
            }
            int64_t timestamp = vb->getTimestamp(probeRow);
            RowKind rowKind = vb->getRowKind(probeRow);
            for (int i = 0; i < targetRowsCnt; i++) {
                output->setRowKind(outputRowIndex, rowKind);
                output->setTimestamp(outputRowIndex++, timestamp);
            }
            rowIndex += targetRowsCnt;
        }
        return output;
    };

    void BuildProbeOutput(
        omnistream::VectorBatch* vb,
        std::vector<std::tuple<int32_t*, int32_t>>& matchedRows,
        int32_t totRowCnt,
        omnistream::VectorBatch* output,
        omniruntime::vec::BaseVector* outCol)
    {
        for (int icol = 0; icol < vb->GetVectorCount(); icol++) {
            auto typeId = vb->Get(icol)->GetTypeId();
            switch (typeId) {
                case omniruntime::type::OMNI_LONG:
                    outCol = buildProbeOutputColumn<int64_t>(vb, matchedRows, totRowCnt, icol);
                    break;
                case omniruntime::type::OMNI_TIMESTAMP:
                case omniruntime::type::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
                case omniruntime::type::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    outCol = buildProbeOutputColumn<int64_t>(vb, matchedRows, totRowCnt, icol);
                    break;
                case omniruntime::type::OMNI_VARCHAR:
                case omniruntime::type::OMNI_CHAR:
                    outCol = buildProbeOutputColumn<omniruntime::vec::LargeStringContainer<std::string_view>>(
                        vb, matchedRows, totRowCnt, icol);
                    break;
                default: std::runtime_error("Type not supported!");
            }
            output->Append(outCol);
        }
    }

    void BuildHashOutput(
        omnistream::VectorBatch* vb,
        std::vector<std::tuple<int32_t*, int32_t>>& matchedRows,
        int32_t totRowCnt,
        omnistream::VectorBatch* output,
        omniruntime::vec::BaseVector* outCol)
    {
        for (size_t icol = 0; icol < src->getTableFieldTypes().size(); icol++) {
            auto typeId = LogicalType::flinkTypeToOmniTypeId(src->getTableFieldTypes()[icol]);
            switch (typeId) {
                case omniruntime::type::OMNI_LONG:
                    outCol = buildHashOutputColumn<int64_t>(matchedRows, totRowCnt, icol);
                    break;
                case omniruntime::type::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
                case omniruntime::type::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                case omniruntime::type::OMNI_TIMESTAMP:
                    outCol = buildHashOutputColumn<int64_t>(matchedRows, totRowCnt, icol);
                    break;
                case omniruntime::type::OMNI_VARCHAR:
                case omniruntime::type::OMNI_CHAR:
                    outCol = buildHashOutputColumn<omniruntime::vec::LargeStringContainer<std::string_view>>(
                        matchedRows, totRowCnt, icol);
                    break;
                default: std::runtime_error("Type not supported!");
            }
            output->Append(outCol);
        }
    }

    template <typename T>
    omniruntime::vec::Vector<T>* buildHashOutputColumn(
        std::vector<std::tuple<int32_t*, int32_t>>& matchedRows, int32_t totRowCnt, int colIndex)
    {
        auto outVec = new omniruntime::vec::Vector<T>(totRowCnt);
        auto inputCol = static_cast<omniruntime::vec::Vector<T>*>(csvDataVecBatch->Get(colIndex));

        int32_t outRowIndex = 0;
        for (size_t probeRow = 0; probeRow < matchedRows.size(); probeRow++) {
            int32_t targetRowsCnt = std::get<1>(matchedRows[probeRow]);
            if (targetRowsCnt == 0) {
                continue; // no match found for this one
            }
            int32_t* targetRowsIndices = std::get<0>(matchedRows[probeRow]);
            for (int32_t i = 0; i < targetRowsCnt; i++) {
                auto value = inputCol->GetValue(targetRowsIndices[i]);
                outVec->SetValue(outRowIndex++, value);
            }
        }
        return outVec;
    }

    template <typename T>
    omniruntime::vec::Vector<T>* buildProbeOutputColumn(
        omnistream::VectorBatch* vb,
        std::vector<std::tuple<int32_t*, int32_t>>& matchedRows,
        int32_t totRowCnt,
        int colIndex)
    {
        auto outVec = new omniruntime::vec::Vector<T>(totRowCnt);
        auto inputCol = static_cast<omniruntime::vec::Vector<T>*>(vb->Get(colIndex));
        int32_t rowIndex = 0;
        for (size_t probeRow = 0; probeRow < matchedRows.size(); probeRow++) {
            int32_t targetRowsCnt = std::get<1>(matchedRows[probeRow]);
            if (targetRowsCnt == 0) {
                continue; // no match found for this one
            }
            auto val = inputCol->GetValue(probeRow);
            for (int i = 0; i < targetRowsCnt; i++) {
                outVec->SetValue(rowIndex + i, val);
            }
            rowIndex += targetRowsCnt;
        }
        return outVec;
    }

    nlohmann::json description;
    CsvTableSource* src;
    std::vector<int32_t> sourceKeys; // probe side
    std::vector<int32_t> targetKeys; // hash side
    emhash7::HashMap<K, std::vector<int32_t>> dataMap;
    omniruntime::vec::VectorBatch* csvDataVecBatch;
    using GetFromStrAndSetToVB = void (*)(const std::string&, omniruntime::vec::BaseVector*, int);
    std::vector<GetFromStrAndSetToVB> csvStrConverters;
};

#endif
