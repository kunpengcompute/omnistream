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

#include "InternalSerializers.h"

#include <stdexcept>

#include "../../core/typeutils/LongSerializer.h"
#include "../types/logical/RowType.h"
#include "../types/logical/RawType.h"
#include "RowDataSerializer.h"
#include "RawValueDataSerializer.h"
#include "StringDataSerializer.h"

using namespace omniruntime::type;

TypeSerializer* InternalSerializers::create(LogicalType* type)
{
    return createInternal(type);
}

TypeSerializer* InternalSerializers::createInternal(LogicalType* type)
{
    switch (type->getTypeId()) {
        case DataTypeId::OMNI_CONTAINER: {
            if (auto* rowType = dynamic_cast<omnistream::RowType*>(type)) {
                return new RowDataSerializer(rowType);
            } else if (auto* rawType = dynamic_cast<omnistream::RawType*>(type)) {
                return new RawValueDataSerializer(rawType->getClassName(), rawType->getSerializerString());
            } else {
                THROW_LOGIC_EXCEPTION(
                    "OMNI_CONTAINER type must be ROW or RAW, actual logical type: " << type->getTypeName());
            }
        }
        case DataTypeId::OMNI_LONG:
            return LongSerializer::INSTANCE; // `LongSerializer` is currently dummy, we use `RowDataSerializer`'s
                                             // `serialize` and `deserialize` for now
        case DataTypeId::OMNI_INT:
            return LongSerializer::INSTANCE; // `LongSerializer` is currently dummy, we use `RowDataSerializer`'s
                                             // `serialize` and `deserialize` for now
        case DataTypeId::OMNI_DOUBLE:
            return LongSerializer::INSTANCE; // DOUBLE is a fixed 8-byte field; reuse the dummy serializer like INT/LONG
        case DataTypeId::OMNI_DATE32:
            return LongSerializer::INSTANCE; // DATE is a fixed-width (int days) field; reuse the dummy serializer like INT
        case DataTypeId::OMNI_DECIMAL64:
            return LongSerializer::INSTANCE; // DECIMAL64 is a fixed 8-byte field stored as long (unscaled value);
                                             // consistent with rowdata_marshaller's SerializeLongIntoRowData
        case DataTypeId::OMNI_DECIMAL128:
            return LongSerializer::INSTANCE; // DECIMAL128 is a fixed 16-byte field stored as Decimal128 (low+high bits);
                                             // consistent with rowdata_marshaller's SerializeDecimal128IntoRowData
            return LongSerializer::INSTANCE; // DATE is a fixed-width (int days) field; reuse the dummy serializer like
                                             // INT
        case DataTypeId::OMNI_BOOLEAN: return LongSerializer::INSTANCE;
        case DataTypeId::OMNI_TIMESTAMP: return LongSerializer::INSTANCE; // TIMESTAMP stored as int64 millis
        case DataTypeId::OMNI_TIME_WITHOUT_TIME_ZONE: return LongSerializer::INSTANCE;
        case DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE: return LongSerializer::INSTANCE;
        case DataTypeId::OMNI_TIMESTAMP_WITH_TIME_ZONE: return LongSerializer::INSTANCE;
        case DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE: return LongSerializer::INSTANCE;
        case DataTypeId::OMNI_VARCHAR: return StringDataSerializer::INSTANCE;
        case DataTypeId::OMNI_CHAR: return StringDataSerializer::INSTANCE;
        default: THROW_LOGIC_EXCEPTION("Unknown type" + std::to_string(type->getTypeId()));
    }
}
