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

#include "basictypes/java_lang_ClassLoader.h"
ClassLoader::ClassLoader() = default;
ClassLoader::~ClassLoader() = default;
InputStream* ClassLoader::getResourceAsStream(String* str)
{
    return nullptr;
};
InputStream* ClassLoader::getResourceAsStream(const std::string& str)
{
    return nullptr;
};
