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

#ifndef OMNISTREAM_USER_H
#define OMNISTREAM_USER_H
#include <string>
#include "basictypes/Long.h"
#include "basictypes/String.h"
#include "basictypes/Class.h"
// todo will clean
class User : public Object {
public:
    User() = default;
    ~User() = default;
    static Class* getClass();
    static Class* clazz_;

    String* name;
    long age;
    int hashCode() override
    {
        return 0;
    }
    bool equals(Object* obj) override
    {
        auto user = static_cast<User*>(obj);
        if (user->name->equals(name) && age == user->age) {
            return true;
        }
        return false;
    }
    std::string toString() override
    {
        return "test_user";
    }
    Object* clone() override
    {
        auto user = new User;
        user->name = static_cast<String*>(name->clone());
        user->age = age;
        return user;
    }
};

#endif // OMNISTREAM_USER_H
