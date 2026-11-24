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

#ifndef ROW_H
#define ROW_H

#include <any>
#include <string>
#include <vector>
#include <map>
#include <memory>
#include <stdexcept>
#include "RowKind.h"
#include "RowData.h"

class Row {
public:
    // 构造函数
    Row(RowKind kind,
        std::vector<std::any> fieldByPosition,
        std::map<std::string, std::any> fieldByName,
        std::map<std::string, int> positionByName);

    RowKind getKind() const;
    void setKind(RowKind kind);
    size_t getArity() const;

    std::any getField(int pos) const;
    std::any getField(const std::string& name) const;

    std::vector<std::any> getFieldByPosition() const
    {
        return this->fieldByPosition_;
    }

    std::map<std::string, std::any> getFieldByName() const
    {
        return this->fieldByName_;
    }

    std::map<std::string, int> getPositionByName() const
    {
        return this->positionByName_;
    }

    void setInternalRow(RowData* internalRow)
    {
        this->internalRow_ = internalRow;
    }

    RowData* getInternalRow()
    {
        return this->internalRow_;
    }

    void clear();

private:
    RowKind kind_;
    std::vector<std::any> fieldByPosition_;
    std::map<std::string, std::any> fieldByName_;
    std::map<std::string, int> positionByName_;
    RowData* internalRow_;
};

#endif // ROW_H
