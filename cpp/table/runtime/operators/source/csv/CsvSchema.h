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

#ifndef CSVSCHEMA_H
#define CSVSCHEMA_H

#pragma once
#include <string>
#include <vector>

#include "type/data_type.h"

namespace omnistream {
namespace csv {

class CsvSchema {
public:
    explicit CsvSchema(const std::vector<omniruntime::type::DataTypeId>& types) : types_(types)
    {
        arity_ = static_cast<int>(types_.size());
        columnSeparator_ = ',';
        quoteChar_ = '"';
        escapeChar_ = '\\';
        allowComments_ = false;
        arrayElementSeparator_ = ",";
        hasNullValue_ = false;
        nullValue_ = "";
    }

    // Setter methods
    void setColumnSeparator(char separator)
    {
        columnSeparator_ = separator;
    }

    void setQuoteChar(char quote)
    {
        quoteChar_ = quote;
    }

    void setAllowComments(bool allow)
    {
        allowComments_ = allow;
    }

    void setArrayElementSeparator(const std::string& separator)
    {
        arrayElementSeparator_ = separator;
    }

    void setEscapeChar(char escape)
    {
        escapeChar_ = escape;
    }

    void setNullValue(const std::string& nullValue)
    {
        hasNullValue_ = true;
        nullValue_ = nullValue;
    }

    // Getter methods
    char getColumnSeparator() const
    {
        return columnSeparator_;
    }

    char getQuoteChar() const
    {
        return quoteChar_;
    }

    char getEscapeChar() const
    {
        return escapeChar_;
    }

    bool getAllowComments() const
    {
        return allowComments_;
    }

    std::string getArrayElementSeparator() const
    {
        return arrayElementSeparator_;
    }

    std::string getNullValue() const
    {
        return nullValue_;
    }

    bool hasNullValue() const
    {
        return hasNullValue_;
    }

    int getArity() const
    {
        return arity_;
    }

    std::vector<omniruntime::type::DataTypeId> getTypes() const
    {
        return types_;
    }

    omniruntime::type::DataTypeId getTypeAtIdx(int idx) const
    {
        return types_[idx];
    }

private:
    char columnSeparator_;
    char quoteChar_;
    char escapeChar_;
    bool allowComments_;
    std::string arrayElementSeparator_;
    bool hasNullValue_;
    std::string nullValue_;

    int arity_;
    std::vector<omniruntime::type::DataTypeId> types_;
};

} // namespace csv
} // namespace omnistream
#endif
