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

#ifndef OMNISTREAM_CONFIGOPTION_H
#define OMNISTREAM_CONFIGOPTION_H
#include "basictypes/Object.h"
#include "basictypes/String.h"

class ConfigOption {
public:
    ConfigOption(String* key, Object* defaultValue, bool hasDefaultValueValue = true)
        : key(key),
          defaultValue(defaultValue),
          hasDefaultValueValue(hasDefaultValueValue)
    {
    }

    ~ConfigOption();

    String* GetKey();

    Object* GetDefaultValue();

    bool hasDefaultValue();

private:
    String* key = nullptr;
    Object* defaultValue = nullptr;
    // todo
    bool isList = false;
    bool hasDefaultValueValue = true;
};

#include <string>
#include <initializer_list>
#include <vector>
#include <optional>
#include "FallbackKey.h"

template <typename T>
class ConfigOptionV2 {
public:
    ConfigOptionV2(std::string key, std::string description, std::optional<T> defaultValue, bool isList)
        : key_(key),
          description_(description),
          defaultValue_(defaultValue),
          isList_(isList) {};

    ConfigOptionV2(
        std::string key,
        std::string description,
        std::optional<T> defaultValue,
        bool isList,
        std::vector<FallbackKey*> list)
        : key_(key),
          description_(description),
          defaultValue_(defaultValue),
          isList_(isList),
          fallbackKeys_(list) {};

    ConfigOptionV2<T>* withDescription(std::string description)
    {
        return new ConfigOptionV2<T>(key_, description, defaultValue_, isList_, fallbackKeys_);
    }

    ConfigOptionV2<T>* withDeprecatedKeys(std::initializer_list<std::string> deprecatedKeys)
    {
        for (auto str : deprecatedKeys) {
            fallbackKeys_.push_back(FallbackKey::createDeprecatedKey(str));
        }
        return new ConfigOptionV2<T>(key_, description_, defaultValue_, isList_, fallbackKeys_);
    }

    std::string key() const
    {
        return key_;
    }

    std::optional<T> defaultValue() const
    {
        return defaultValue_;
    }

    bool hasDefaultValue() const
    {
        return defaultValue_.has_value();
    }

    bool hasFallbackKeys() const
    {
        return !fallbackKeys_.empty();
    }

private:
    std::string key_;
    std::string description_;
    std::optional<T> defaultValue_;
    bool isList_;
    std::vector<FallbackKey*> fallbackKeys_;
};

#endif // OMNISTREAM_CONFIGOPTION
