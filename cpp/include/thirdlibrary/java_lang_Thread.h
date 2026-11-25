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

#ifndef MT_CHECK_JAVA_LANG_THREAD_H
#define MT_CHECK_JAVA_LANG_THREAD_H

#include <thread>
#include "basictypes/String.h"

class java_lang_Thread : public std::thread, public Object {
public:
    java_lang_Thread();
    java_lang_Thread(std::thread::id id);
    ~java_lang_Thread();

    static void sleep(int64_t timeMill);
    static java_lang_Thread* currentThread();
    String getName();
    void setName(const std::string& name);
    int64_t getId();

private:
    std::thread::id id;
    std::string name;
    static std::map<std::thread::id, std::string> threadNames;
};

#endif // MT_CHECK_JAVA_LANG_THREAD_H
