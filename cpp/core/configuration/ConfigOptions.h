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

#ifndef OMNISTREAM_CONFIGOPTIONS
#define OMNISTREAM_CONFIGOPTIONS

#include <string>
#include <chrono>
#include <optional>
#include "ConfigOption.h"

template <typename T>
class TypedConfigOptionBuilder {
public:
    explicit TypedConfigOptionBuilder(std::string key) : key(key) {};
    ConfigOptionV2<T>* defaultValue(T value)
    {
        return new ConfigOptionV2<T>(key, "", value, false);
    }
    ConfigOptionV2<T>* noDefaultValue()
    {
        return new ConfigOptionV2<T>(key, "", std::nullopt, false);
    }

private:
    std::string key;
};

class OptionBuilder {
public:
    explicit OptionBuilder(std::string key);
    // Main one being used
    TypedConfigOptionBuilder<bool>* booleanType();

    TypedConfigOptionBuilder<int>* intType();
    TypedConfigOptionBuilder<long>* longType();
    TypedConfigOptionBuilder<float>* floatType();
    TypedConfigOptionBuilder<double>* doubleTYpe();
    TypedConfigOptionBuilder<std::string>* stringType();
    TypedConfigOptionBuilder<long>* memoryType();
    TypedConfigOptionBuilder<std::chrono::milliseconds>* durationType();
    template <typename T>
    TypedConfigOptionBuilder<T>* enumType();

    template <typename T>
    ConfigOptionV2<T>* defaultValue(T value)
    {
        return new ConfigOptionV2<T>(key, "", value, false);
    }

private:
    std::string key;
};

class ConfigOptions {
public:
    static OptionBuilder* key(std::string key);
};

template <typename T>
inline TypedConfigOptionBuilder<T>* OptionBuilder::enumType()
{
    return new TypedConfigOptionBuilder<T>(key);
}

#endif // OMNISTREAM_CONFIGOPTIONS
