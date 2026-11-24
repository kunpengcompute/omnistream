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

#include "table/types/logical/DataType.h"

namespace omnistream {
// Since most methods are inline or pure virtual,
// we only need this empty implementation file as a placeholder
// for potential future non-inline implementations

DataType::DataType(std::shared_ptr<LogicalType> logicalType, std::shared_ptr<void> conversionClass)
    : logicalType_(logicalType),
      conversionClass_(ensureConversionClass(logicalType, conversionClass))
{
    performEarlyClassValidation(logicalType, conversionClass_);
}

std::shared_ptr<LogicalType> DataType::getLogicalType() const
{
    return logicalType_;
}

std::shared_ptr<void> DataType::getConversionClass() const
{
    return conversionClass_;
}
} // namespace omnistream
