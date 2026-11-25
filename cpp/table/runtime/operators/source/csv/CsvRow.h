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

#ifndef CSVROW_H
#define CSVROW_H

#pragma once

#include "CsvNode.h"
#include "CsvSchema.h"
#include <vector>
#include <string>
#include <sstream>

namespace omnistream {
namespace csv {

class CsvRow {
public:
    explicit CsvRow(const std::string& line, const CsvSchema& schema) : innerSchema(schema)
    {
        parseLine(line, schema);
    }

    const std::vector<std::shared_ptr<CsvNode>>& getNodes() const
    {
        return nodes_;
    }

    CsvSchema getSchema() const
    {
        return innerSchema;
    }

private:
    std::vector<std::shared_ptr<CsvNode>> nodes_;
    CsvSchema innerSchema;

    void parseLine(const std::string& line, const CsvSchema& schema)
    {
        char delimiter = schema.getColumnSeparator();
        char quoteChar = schema.getQuoteChar();
        std::string field;
        bool insideQuotes = false;

        int idx = 0;
        for (size_t i = 0; i < line.size(); ++i) {
            char c = line[i];
            if (c == quoteChar) {
                if (insideQuotes) {
                    if (i + 1 < line.size() && line[i + 1] == quoteChar) {
                        field += quoteChar;
                        ++i;
                    } else if (i + 1 == line.size() || line[i + 1] == delimiter) {
                        insideQuotes = false;
                    } else {
                        field += c;
                    }
                } else if (field.empty()) {
                    insideQuotes = true;
                } else {
                    field += c;
                }
            } else if (c == delimiter && !insideQuotes) {
                nodes_.emplace_back(std::make_shared<CsvNode>(field, schema.getTypeAtIdx(idx++)));
                field.clear();
            } else {
                field += c;
            }
        }
        nodes_.emplace_back(std::make_shared<CsvNode>(field, schema.getTypeAtIdx(idx++)));
    }
};

} // namespace csv
} // namespace omnistream
#endif
