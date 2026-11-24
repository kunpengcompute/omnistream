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

#include <memory>
#include <vector>
#include <string>
#include "table/types/logical/LogicalType.h"

class DataTypeVisitor;

namespace omnistream {
/**
 * Describes the data type of a value in the table ecosystem. Instances of this class can be used to
 * declare input and/or output types of operations.
 *
 * The DataType class has two responsibilities: declaring a logical type and giving hints
 * about the physical representation of data to the planner.
 */
class DataType {
public:
    DataType(std::shared_ptr<LogicalType> logicalType, std::shared_ptr<void> conversionClass = nullptr);

    virtual ~DataType() = default;

    /**
     * Returns the corresponding logical type.
     */
    std::shared_ptr<LogicalType> getLogicalType() const;

    /**
     * Returns the corresponding conversion class for representing values.
     */
    std::shared_ptr<void> getConversionClass() const;

    /**
     * Returns the child data types if this type is a composite type.
     */
    virtual std::vector<std::shared_ptr<DataType>> getChildren() const = 0;

    /**
     * Accepts a visitor to allow for type-specific handling.
     */
    virtual void accept(DataTypeVisitor& visitor) = 0;

    virtual std::string toString() const = 0;

    bool operator==(const DataType& other) const;

protected:
    std::shared_ptr<LogicalType> logicalType_;
    std::shared_ptr<void> conversionClass_;

private:
    static void performEarlyClassValidation(std::shared_ptr<LogicalType> logicalType, std::shared_ptr<void> candidate) {
    };

    static std::shared_ptr<void> ensureConversionClass(
        std::shared_ptr<LogicalType> logicalType, std::shared_ptr<void> clazz)
    {
        return nullptr;
    };
};
} // namespace omnistream
