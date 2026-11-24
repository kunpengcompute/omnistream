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

#ifndef FLINK_TNEL_TYPEINFOFACTORY_H
#define FLINK_TNEL_TYPEINFOFACTORY_H

#include <string>
#include <nlohmann/json.hpp>
#include "TypeInformation.h"
#include "BasicTypeInfo.h"
#include "TupleTypeInfo.h"
#include "table/types/logical/LogicalType.h"
#include "types/logical/RowType.h"
#include "basictypes/Object.h"

// for convenience
using json = nlohmann::json;
using string = std::string;

class TypeInfoFactory : public Object {
public:
    static TypeInformation* createTypeInfo(const char* name);

    // internalType  is mapped from java table internal type, which is for sql type
    static TypeInformation* createInternalTypeInfo(const nlohmann::json& rowType);

    static TypeInformation* createInternalTypeInfoOfRow(const nlohmann::json& fields);

    // Tuple
    static TypeInformation* createTupleTypeInfo(const nlohmann::json& rowType);

    static TypeInformation* createCommittableMessageInfo();

    static TypeInformation* createDataStreamTypeInfo(const nlohmann::json& serializerInfo);

    // may not be  useful, leave it as it for now. basic internal type is mapped from java sql type,
    // logicaltyperoot.PREDEFINED,
    static TypeInformation* createBasicInternalTypeInfo(const char* name);

    static LogicalType* createLogicalType(const std::string type);

    static omnistream::RowType* createRowType(const std::vector<omniruntime::type::DataTypeId>* inputRowType);

    static LogicalType* createDataType(const nlohmann::json& logicalTypeJson);
};

#endif
