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

#ifndef FLINK_TNEL_TYPEINFORMATION_H
#define FLINK_TNEL_TYPEINFORMATION_H

#include <string>
#include "typeutils/TypeSerializer.h"
#include "basictypes/Class.h"

class TypeInformation : public Object {
public:
    // TypeInformation(int id) : dataId(id) {}

    virtual TypeSerializer* createTypeSerializer() = 0;
    virtual TypeSerializer* getTypeSerializer();
    virtual std::string name() = 0;

    virtual ~TypeInformation();

    virtual BackendDataType getBackendId() const = 0;

    static TypeInformation* of(Class* cl);
};

#endif
