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

#ifndef OMNISTREAM_LISTTYPEINFO_H
#define OMNISTREAM_LISTTYPEINFO_H
#include "TypeInformation.h"
#include "TypeExtractor.h"
#include "basictypes/Class.h"

class ListTypeInfo : public TypeInformation {
public:
    explicit ListTypeInfo(TypeInformation* elementTypeInfo) : elementTypeInfo(elementTypeInfo)
    {
    }

    explicit ListTypeInfo(Class* cl)
    {
        elementTypeInfo = TypeExtractor::CreateTypeInfo(cl);
    }

    ~ListTypeInfo() override
    {
        elementTypeInfo->putRefCount();
    }

    TypeSerializer* createTypeSerializer() override;
    BackendDataType getBackendId() const override
    {
        return BackendDataType::OBJECT_BK;
    }
    std::string name() override
    {
        return "ListTypeInfo";
    }

private:
    TypeInformation* elementTypeInfo;
};

#endif // OMNISTREAM_LISTTYPEINFO_H
