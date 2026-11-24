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

#include "thirdlibrary/org_slf4j_Logger.h"
#include <iostream>

Logger::Logger() = default;
Logger::~Logger() = default;
void Logger::info(String* info)
{
    std::cout << "this is custom logger: " << info->toString() << std::endl;
}

void Logger::info(String* info, Object* obj1)
{
    std::cout << "this is custom logger: " << info->toString() << std::endl;
}

void Logger::info(String* info, Object* obj1, Object* obj2)
{
    std::cout << "this is custom logger: " << info->toString() << std::endl;
}

void Logger::info(String* format, String* info)
{
    std::cout << "this is custom logger: " << format->toString() << info->toString() << std::endl;
}

void Logger::warn(String* warn)
{
    std::cout << "this is custom logger: " << warn->toString() << std::endl;
}

void Logger::warn(String* warn, java_lang_Throwable* e)
{
    std::cout << "this is custom logger: " << warn->toString() << std::endl;
}

void Logger::error(String* error)
{
    std::cout << "this is custom logger: " << error->toString() << std::endl;
}

void Logger::error(String* error, java_lang_Throwable* e)
{
    std::cout << "this is custom logger: " << error->toString() << std::endl;
}

void Logger::info(const std::string& info)
{
    std::cout << "this is custom logger: " << info << std::endl;
}

void Logger::info(const std::string& info, Object* obj1)
{
    std::cout << "this is custom logger: " << info << std::endl;
}

void Logger::info(const std::string& info, Object* obj1, Object* obj2)
{
    std::cout << "this is custom logger: " << info << std::endl;
}

void Logger::info(const std::string& format, const std::string& info)
{
    std::cout << "this is custom logger: " << format << info << std::endl;
}

void Logger::warn(const std::string& warn)
{
    std::cout << "this is custom logger: " << warn << std::endl;
}

void Logger::warn(const std::string& warn, java_lang_Throwable* e)
{
    std::cout << "this is custom logger: " << warn << std::endl;
}

void Logger::error(const std::string& error)
{
    std::cout << "this is custom logger: " << error << std::endl;
}

void Logger::error(const std::string& error, java_lang_Throwable* e)
{
    std::cout << "this is custom logger: " << error << std::endl;
}
