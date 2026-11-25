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

#ifndef ME_CHECK_ARRAYS_H
#define ME_CHECK_ARRAYS_H

#include "java_util_List.h"
#include "Object.h"
#include "Array.h"
#include "JavaArray.h"

class Arrays : public Object {
public:
    static List* asList(Array* obj);
};
#endif // ME_CHECK_ARRAYS_H
