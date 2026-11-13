/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include <stdexcept>
#include "RowData.h"
#include "../../core/include/common.h"
#include "types/logical/TimeWithoutTimeZoneType.h"
#include "types/logical/TimestampWithoutTimeZoneType.h"
#include "types/logical/TimestampWithTimeZoneType.h"
#include "types/logical/TimestampWithLocalTimeZoneType.h"
#include "types/logical/RawType.h"

using namespace omniruntime::type;

FieldGetter* RowData::createFieldGetter(LogicalType* fieldType, int fieldPos)
{
    if (!fieldType) {
        throw std::runtime_error("field type is null");
    }
    switch (fieldType->getTypeId()) {
        case DataTypeId::OMNI_INT:
            return new FieldGetter(fieldPos, reinterpret_cast<getFieldByPosFn>(&RowData::getInt));
        case DataTypeId::OMNI_LONG:
            return new FieldGetter(fieldPos, reinterpret_cast<getFieldByPosFn>(&RowData::getLong));
        case DataTypeId::OMNI_DECIMAL64:
            return new FieldGetter(fieldPos, reinterpret_cast<getFieldByPosFn>(&RowData::getLong));
        case DataTypeId::OMNI_DECIMAL128:
            return new FieldGetter(fieldPos, reinterpret_cast<getFieldByPosFn>(&RowData::getDecimal128));
        case DataTypeId::OMNI_DOUBLE:
            return new FieldGetter(fieldPos, reinterpret_cast<getFieldByPosFn>(&RowData::getDouble));
        case DataTypeId::OMNI_DATE32:
            return new FieldGetter(fieldPos, reinterpret_cast<getFieldByPosFn>(&RowData::getInt));
        case DataTypeId::OMNI_BOOLEAN:
            return new FieldGetter(fieldPos, reinterpret_cast<getFieldByPosFn>(&RowData::getBool));
        case DataTypeId::OMNI_VARCHAR:
            return new FieldGetter(fieldPos, reinterpret_cast<getFieldByPosFn>(&RowData::getStringView));
        case DataTypeId::OMNI_CHAR:
            return new FieldGetter(fieldPos, reinterpret_cast<getFieldByPosFn>(&RowData::getStringView));
        case DataTypeId::OMNI_TIMESTAMP:
            return new FieldGetter(fieldPos, false);
        case DataTypeId::OMNI_TIME_WITHOUT_TIME_ZONE: return new FieldGetter(fieldPos, false);
        case DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE: {
            if (static_cast<TimestampWithoutTimeZoneType>(fieldType).getPrecision() > 3) {
                return new FieldGetter(fieldPos, true);
            }
            return new FieldGetter(fieldPos, false);
        }
        case DataTypeId::OMNI_TIMESTAMP_WITH_TIME_ZONE: {
            if (static_cast<TimestampWithTimeZoneType>(fieldType).getPrecision() > 3) {
                return new FieldGetter(fieldPos, true);
            }
            return new FieldGetter(fieldPos, false);
        }
        case DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE: {
            if (static_cast<TimestampWithLocalTimeZoneType>(fieldType).getPrecision() > 3) {
                return new FieldGetter(fieldPos, true);
            }
            return new FieldGetter(fieldPos, false);
        }
        case DataTypeId::OMNI_CONTAINER:
            if (dynamic_cast<omnistream::RawType*>(fieldType) != nullptr) {
                return new FieldGetter(fieldPos, &RowData::getRawValue);
            }
            THROW_LOGIC_EXCEPTION("Unsupported OMNI_CONTAINER field type: " << fieldType->getTypeName());
        default: THROW_LOGIC_EXCEPTION("Unknown type: " + std::to_string(fieldType->getTypeId()));
    }
}

RowData::RowData(int rowDataTypeId) : rowDataTypeID_(rowDataTypeId)
{
}

int RowData::getRowDataTypeId() const
{
    return rowDataTypeID_;
}

/**
 * Implement here becuase there's no definition for inherited class `GenericRowData` and `JoinedRowData`
 * Override in `GenericRowData` and `JoinedRowData` and delete this interface later
 */
BinaryStringData* RowData::getString(int pos)
{
    return nullptr;
}

void RowData::printRow()
{
    std::cout << "row:";
    for (int i = 0; i < getArity(); i++) {
        if (isNullAt(i)) {
            std::cout << "null|";
        } else {
            std::cout << *getLong(i) << "|";
        }
    }
    std::cout << std::endl;
}
