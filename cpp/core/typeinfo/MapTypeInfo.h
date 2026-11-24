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

#ifndef FLINK_TNEL_MAPTYPEINFO_H
#define FLINK_TNEL_MAPTYPEINFO_H

#include "../typeutils/TypeSerializer.h"
#include "TypeInformation.h"
#include "typeconstants.h"
#include "core/typeutils/MapSerializer.h"

class MapTypeInfo : public TypeInformation {
public:
    explicit MapTypeInfo(TypeInformation* keyInfo, TypeInformation* valueInfo)
        : keyTypeInfo(keyInfo),
          valueTypeInfo(valueInfo)
    {
        if (keyInfo != nullptr) {
            keyInfo->getRefCount();
        }
        if (valueInfo != nullptr) {
            valueInfo->getRefCount();
        }
    }

    ~MapTypeInfo() override
    {
        if (keyTypeInfo != nullptr) {
            keyTypeInfo->putRefCount();
        }
        if (valueTypeInfo != nullptr) {
            valueTypeInfo->putRefCount();
        }
    }

    bool isBasicType() const
    {
        return false;
    }

    bool isTupleType() const
    {
        return false;
    }

    int getArity() const
    {
        return 0;
    }

    int getTotalFields() const
    {
        return 1;
    }

    bool isKeyType()
    {
        return false;
    }

    std::string name() override
    {
        return keyTypeInfo->name() + " " + valueTypeInfo->name();
    }

    TypeSerializer* createTypeSerializer() override;

    BackendDataType getBackendId() const override
    {
        return BackendDataType::OBJECT_BK;
    };

private:
    const char* name_ = TYPE_NAME_STRING;
    TypeInformation* keyTypeInfo;
    TypeInformation* valueTypeInfo;
};

#endif // FLINK_TNEL_STRINGTYPEINFO_H
