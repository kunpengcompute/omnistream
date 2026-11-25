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

#ifndef ME_CHECK_JAVAARRAY_H
#define ME_CHECK_JAVAARRAY_H

#include "basictypes/java_util_List.h"
#include "basictypes/Array.h"
#include "basictypes/Object.h"

class JavaArrays : public Object {
public:
    static List* asList(Array* obj);

    static List* asList(Object* obj);
};
#endif // ME_CHECK_JAVAARRAY_H
