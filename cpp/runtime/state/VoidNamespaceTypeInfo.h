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

#ifndef OMNISTREAM_VOIDNAMESPACETYPEINFO_H
#define OMNISTREAM_VOIDNAMESPACETYPEINFO_H

#include "core/typeinfo/TypeInformation.h"
#include "core/typeinfo/typeconstants.h"

class VoidNamespaceTypeInfo : public TypeInformation {
public:
    explicit VoidNamespaceTypeInfo(const char* name);

    TypeSerializer* createTypeSerializer() override;

    std::string name() override;

    BackendDataType getBackendId() const override
    {
        return BackendDataType::VOID_NAMESPACE_BK;
    }

private:
    const char* name_ = TYPE_NAME_VOID_NAMESPACE;
};

#endif // OMNISTREAM_VOIDNAMESPACETYPEINFO_H
