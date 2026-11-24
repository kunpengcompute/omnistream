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

#include "BinaryWriter.h"
#include "table/data/binary/BinaryRowData.h"
#include "../../types/logical/RawType.h"
#include "table/data/TimestampData.h"

using namespace omniruntime::type;

void BinaryWriter::write(BinaryWriter* writer, int pos, void* object, LogicalType* type, TypeSerializer* serializer)
{
    switch (type->getTypeId()) {
        case DataTypeId::OMNI_BOOLEAN: writer->writeLong(pos, *(reinterpret_cast<bool*>(object))); break;
        case DataTypeId::OMNI_LONG: writer->writeLong(pos, *(reinterpret_cast<long*>(object))); break;
        case DataTypeId::OMNI_TIME_WITHOUT_TIME_ZONE:
        case DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        case DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
            writer->writeLong(pos, reinterpret_cast<TimestampData*>(object)->getMillisecond());
            break;
        case DataTypeId::OMNI_CONTAINER: {
            if (dynamic_cast<omnistream::RawType*>(type) == nullptr) {
                THROW_LOGIC_EXCEPTION("Unsupported OMNI_CONTAINER type: " << type->getTypeName());
            }
            if (object == nullptr) {
                throw std::invalid_argument("Non-null RAW field has null value");
            }
            auto* raw = static_cast<BinaryRawValueData*>(object);
            writer->writeRawValue(pos, raw->data(), raw->size());
            break;
        }
        default: THROW_LOGIC_EXCEPTION("Unknown type" + std::to_string(type->getTypeId()));
    }
}
