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

#include "basictypes/String.h"
#include "thirdlibrary/java_lang_System.h"
long java_lang_System::currentTimeMillis()
{
    timeval time;
    gettimeofday(&time, nullptr);
    return time.tv_sec * 1000 + time.tv_usec / 1000;
}

String* java_lang_System::getProperty(String* key)
{
    if (key == nullptr) {
        return nullptr;
    }

    const char* value = getenv(key->toString().c_str());
    if (value == nullptr) {
        return nullptr;
    }
    return new String(std::string(value));
}
