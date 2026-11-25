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

#include "ConfigOptions.h"

OptionBuilder::OptionBuilder(std::string key) : key(key)
{
}

TypedConfigOptionBuilder<bool>* OptionBuilder::booleanType()
{
    return new TypedConfigOptionBuilder<bool>(key);
}

TypedConfigOptionBuilder<int>* OptionBuilder::intType()
{
    return new TypedConfigOptionBuilder<int>(key);
}

TypedConfigOptionBuilder<long>* OptionBuilder::longType()
{
    return new TypedConfigOptionBuilder<long>(key);
}

TypedConfigOptionBuilder<float>* OptionBuilder::floatType()
{
    return new TypedConfigOptionBuilder<float>(key);
}

TypedConfigOptionBuilder<double>* OptionBuilder::doubleTYpe()
{
    return new TypedConfigOptionBuilder<double>(key);
}

TypedConfigOptionBuilder<std::string>* OptionBuilder::stringType()
{
    return new TypedConfigOptionBuilder<std::string>(key);
}

TypedConfigOptionBuilder<long>* OptionBuilder::memoryType()
{
    return new TypedConfigOptionBuilder<long>(key);
}

TypedConfigOptionBuilder<std::chrono::milliseconds>* OptionBuilder::durationType()
{
    return new TypedConfigOptionBuilder<std::chrono::milliseconds>(key);
}

OptionBuilder* ConfigOptions::key(std::string key)
{
    return new OptionBuilder(key);
}
