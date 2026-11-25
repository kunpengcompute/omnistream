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

#ifndef MT_CHECK_JAVA_LANG_SYSTEM_H
#define MT_CHECK_JAVA_LANG_SYSTEM_H

#include <sys/time.h>

class java_lang_System {
public:
    static int64_t currentTimeMillis();
    static String* getProperty(String* key);
};

#endif // MT_CHECK_JAVA_LANG_SYSTEM_H
