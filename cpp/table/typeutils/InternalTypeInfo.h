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

#ifndef FLINK_TNEL_INTERNALTYPEINFO_H
#define FLINK_TNEL_INTERNALTYPEINFO_H

#include "../types/logical/LogicalType.h"
#include "../../core/typeutils/TypeSerializer.h"
#include "../../core/typeinfo/TypeInformation.h"
#include "../types/logical/RowType.h"

class InternalTypeInfo : public TypeInformation {
public:
    InternalTypeInfo(LogicalType* type, TypeSerializer* typeSerializer);

    TypeSerializer* createTypeSerializer() override;

    std::string name() override;

    ~InternalTypeInfo() override;

    static InternalTypeInfo* of(LogicalType* type);

    static InternalTypeInfo* ofRowType(omnistream::RowType* type);

    static InternalTypeInfo* INT_TYPE;

    TypeSerializer* getTypeSerializer() override
    {
        return typeSerializer;
    }

    BackendDataType getBackendId() const override
    {
        if (typeSerializer != nullptr) {
            return typeSerializer->getBackendId();
        } else {
            return BackendDataType::INVALID_BK;
        }
    }

private:
    LogicalType* type_;
    TypeSerializer* typeSerializer;
};

#endif
