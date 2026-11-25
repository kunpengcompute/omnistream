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

#ifndef JAVA_UTIL_MAP_ENTRY
#define JAVA_UTIL_MAP_ENTRY
#include "Object.h"

class MapEntry : public Object {
public:
    MapEntry(Object* key, Object* value);

    MapEntry();

    ~MapEntry();

    virtual Object* getKey() const;

    virtual const Object* getValue() const;

    virtual Object* getKey();

    virtual Object* getValue();

    virtual void setValue(Object* newValue);

private:
    Object* key_;
    Object* value_;
};
#endif
