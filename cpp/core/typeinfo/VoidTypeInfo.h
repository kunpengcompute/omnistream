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

#ifndef VOIDTYPEINFO_H
#define VOIDTYPEINFO_H

#include "../typeutils/VoidSerializer.h"
#include "TypeInformation.h"
#include "typeconstants.h"

class VoidTypeInfo : public TypeInformation {
public:
    explicit VoidTypeInfo(const char* name);
    TypeSerializer* createTypeSerializer() override;
    std::string name() override;
    BackendDataType getBackendId() const override
    {
        return BackendDataType::OBJECT_BK;
    }

private:
    const char* name_ = TYPE_NAME_VOID;
};

#endif // VOIDTYPEINFO_H
