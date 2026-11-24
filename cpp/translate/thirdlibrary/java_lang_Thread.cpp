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

#include "thirdlibrary/java_lang_Thread.h"

java_lang_Thread::java_lang_Thread() : std::thread(), id(std::this_thread::get_id()), name("DefaultThread")
{
    threadNames[id] = name;
}

java_lang_Thread::java_lang_Thread(std::thread::id id_) : std::thread(), id(id_), name("DefaultThread")
{
    threadNames[id] = name;
}

java_lang_Thread::~java_lang_Thread()
{
    threadNames.erase(id);
}

java_lang_Thread* java_lang_Thread::currentThread()
{
    static thread_local java_lang_Thread* current = nullptr;
    if (current == nullptr) {
        current = new java_lang_Thread(std::this_thread::get_id());
    }
    return current;
}

String java_lang_Thread::getName()
{
    return String(name);
}

int64_t java_lang_Thread::getId()
{
    return static_cast<int64_t>(std::hash<std::thread::id>()(id));
}

void java_lang_Thread::setName(const std::string& name)
{
    this->name = name;
    threadNames[id] = name;
}

void java_lang_Thread::sleep(int64_t timeMill)
{
    std::this_thread::sleep_for(std::chrono::milliseconds(timeMill));
}

std::map<std::thread::id, std::string> java_lang_Thread::threadNames = {};
