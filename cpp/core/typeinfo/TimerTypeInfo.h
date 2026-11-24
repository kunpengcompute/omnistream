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

#include "TypeInformation.h"
#include "typeconstants.h"
#include "table/runtime/operators/TimerSerializer.h"

class TimerTypeInfo : public TypeInformation {
public:
    explicit TimerTypeInfo(
        TypeInformation* keyTypeInfo_, TypeInformation* namespaceTypeInfo_, Class* keyClazz_, Class* namespaceClazz_);

    ~TimerTypeInfo() override;

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
        return keyTypeInfo->name() + " " + namespaceTypeInfo->name();
    }

    TypeSerializer* createTypeSerializer() override;

    BackendDataType getBackendId() const override
    {
        return BackendDataType::OBJECT_BK;
    }

private:
    const char* name_ = TYPE_NAME_TIMER_SERIALIZER;
    TypeInformation* keyTypeInfo;
    TypeInformation* namespaceTypeInfo;

    Class* keyClazz;
    Class* namespaceClazz;
};
