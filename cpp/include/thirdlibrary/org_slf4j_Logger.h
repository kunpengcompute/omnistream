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

#ifndef FLINK_TNEL_ORG_SLF4J_LOGGER_H
#define FLINK_TNEL_ORG_SLF4J_LOGGER_H
#include "basictypes/Object.h"
#include "basictypes/String.h"
#include "java_lang_Throwable.h"
class Logger : public Object {
public:
    Logger();
    ~Logger();

    void info(String* info);
    void info(String* info, Object* obj1);
    void info(String* info, Object* obj1, Object* obj2);
    void info(String* format, String* info);
    void warn(String* warn);
    void warn(String* warn, java_lang_Throwable* e);
    void warn(String* str, Object* obj1, Object* obj2);
    void error(String* error, java_lang_Throwable* e);
    void error(String* error);

    void info(const std::string& info);
    void info(const std::string& info, Object* obj1);
    void info(const std::string& info, Object* obj1, Object* obj2);
    void info(const std::string& format, const std::string& info);
    void warn(const std::string& warn);
    void warn(const std::string& warn, java_lang_Throwable* e);
    void warn(const std::string& str, Object* obj1, Object* obj2);
    void error(const std::string& error, java_lang_Throwable* e);
    void error(const std::string& str);
};
#endif // FLINK_TNEL_ORG_SLF4J_LOGGER_H
