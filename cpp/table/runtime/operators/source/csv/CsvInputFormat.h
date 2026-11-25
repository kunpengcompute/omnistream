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

#pragma once

#include <fstream>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "CsvRow.h"
#include "CsvSchema.h"
#include "CsvConverter.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "runtime/operators/source/InputSplit.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/types/logical/DataType.h"
#include "table/types/logical/RowType.h"

namespace omnistream {
namespace csv {

template <typename OUT>
class CsvInputFormat {
public:
    CsvInputFormat(const CsvSchema& csvSchema, size_t batchSize) : csvSchema_(csvSchema), batchSize_(batchSize)
    {
        // If mapping is not provided, create a dummy mapping that maps all fields to themselves
        mapping_.resize(csvSchema.getArity());
        for (int i = 0; i < csvSchema.getArity(); i++) {
            mapping_[i] = i;
        }
    }

    CsvInputFormat(const CsvSchema& csvSchema, size_t batchSize, std::vector<int> mapping)
        : csvSchema_(csvSchema),
          batchSize_(batchSize),
          mapping_(mapping)
    {
    }

    ~CsvInputFormat() {};

    void open(InputSplit* split)
    {
        std::string filePath = split->getFilePath();
        // Convert URI format to local file path (e.g., "file:/tmp/data.csv" -> "/tmp/data.csv")
        if (filePath.find("file:") == 0) {
            filePath = filePath.substr(5);
            // Handle "file:///path" (three slashes) or "file:/path" (one slash)
            while (!filePath.empty() && filePath[0] == '/') {
                // Keep removing leading slashes until we have exactly one
                if (filePath.size() > 1 && filePath[1] == '/') {
                    filePath = filePath.substr(1);
                } else {
                    break;
                }
            }
            // Ensure path starts with /
            if (filePath.empty() || filePath[0] != '/') {
                filePath = "/" + filePath;
            }
        }
        inputStream_.open(filePath, std::ios::in | std::ios::binary);
        if (!inputStream_.is_open()) {
            std::cerr << "Failed to open file: " << filePath << " (original: " << split->getFilePath() << ")"
                      << std::endl;
            return;
        }
        inputStream_.seekg(split->getStartOffset());
        endPosition_ = split->getStartOffset() + split->getLength();
        INFO_RELEASE("End position set to: " << endPosition_);
    }

    void close()
    {
        inputStream_.close();
    }

    bool reachedEnd()
    {
        return inputStream_.eof();
    }

    OUT* nextRecord()
    {
        if (!inputStream_.is_open()) {
            std::cerr << "File is not open" << std::endl;
            return nullptr;
        }

        std::vector<CsvRow> rows;
        std::string line;
        size_t lineCount = 0;

        while (lineCount < batchSize_ && !inputStream_.eof()) {
            std::getline(inputStream_, line);
            if (line.empty()) { // skip empty lines
                continue;
            }
            if (inputStream_.fail()) {
                //                std::cerr << "Failed to read line" << std::endl;
                break;
            }
            CsvRow csvRow(line, csvSchema_);
            rows.push_back(csvRow);
            lineCount++;
        }

        if (rows.empty()) {
            return nullptr;
        }

        return CsvConverter::convert(rows, mapping_);
    }

    CsvRow* nextCsvRecord()
    {
        if (!inputStream_.is_open()) {
            std::cerr << "File is not open" << std::endl;
            return nullptr;
        }

        std::string line;
        std::streamoff currentPos = inputStream_.tellg();
        if (static_cast<std::streamoff>(currentPos) >= endPosition_ || inputStream_.eof()) {
            return nullptr;
        }

        std::getline(inputStream_, line);
        if (inputStream_.fail()) {
            std::cerr << "Failed to read line" << std::endl;
            return nullptr;
        }

        std::cout << "Read line: " << line << std::endl;
        return new CsvRow(line, csvSchema_);
    }

    CsvSchema csvSchema_;

private:
    size_t batchSize_;

    // Member variables matching Java implementation
    std::vector<DataType> fieldTypes_;
    std::vector<std::string> fieldNames_;
    std::vector<int> selectFields_;
    std::vector<std::string> partitionKeys_;
    std::string defaultPartValue_;
    int64_t limit_;
    int64_t emitted_;
    std::vector<int> mapping_;
    bool ignoreParseErrors_;

    // File handling members

    std::ifstream inputStream_;
    std::streamoff endPosition_;

private:
};

} // namespace csv
} // namespace omnistream
