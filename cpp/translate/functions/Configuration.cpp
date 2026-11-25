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

#include "functions/Configuration.h"

Configuration::Configuration()
{
    confData = new HashMap();
}

Configuration::Configuration(bool standardYaml) : standardYaml(standardYaml)
{
}

Configuration::Configuration(const Configuration& other)
{
    standardYaml = other.standardYaml;
}

Configuration::~Configuration()
{
    delete confData;
}

void Configuration::setString(std::string key, String* value)
{
    // todo implement Configuration::setString and add libInterfaceRefs
}

void Configuration::setString(std::string key, std::string value)
{
    // todo implement Configuration::setString and add libInterfaceRefs
}

void Configuration::setString(String* key, std::string value)
{
    // todo implement Configuration::setString and add libInterfaceRefs
}

void Configuration::setString(String* key, String* value)
{
    // todo implement Configuration::setString and add libInterfaceRefs
}

void Configuration::setInteger(std::string key, int32_t value)
{
    // todo implement Configuration::setInteger and add libInterfaceRefs
}

void Configuration::setBoolean(std::string key, bool value)
{
    // todo implement Configuration::setBoolean and add libInterfaceRefs
}

bool Configuration::containsKey(std::string key)
{
    // todo implement Configuration::containsKey and add libInterfaceRefs
    return {};
}

String* Configuration::getString(std::string key, std::string defaultValue)
{
    // todo implement Configuration::getString and add libInterfaceRefs
    return {};
}

String* Configuration::getString(String* key, std::string defaultValue)
{
    // todo implement Configuration::getString and add libInterfaceRefs
    return {};
}

String* Configuration::getString(std::string key, String* defaultValue)
{
    // todo implement Configuration::getString and add libInterfaceRefs
    return {};
}

String* Configuration::getString(String* key, String* defaultValue)
{
    // todo implement Configuration::getString and add libInterfaceRefs
    return {};
}

int32_t Configuration::getInteger(std::string key, int32_t defaultValue)
{
    // todo implement Configuration::getInteger and add libInterfaceRefs
    return {};
}

bool Configuration::getBoolean(std::string key, bool defaultValue)
{
    // todo implement Configuration::getBoolean and add libInterfaceRefs
    return {};
}

HashMap* Configuration::getMap()
{
    return confData;
}

Configuration* Configuration::TM_CONFIG = new Configuration();

Object* Configuration::getValue(ConfigOption* configOption)
{
    if (confData == nullptr) {
        return returnDefaultValue(configOption);
    }
    auto str = reinterpret_cast<String*>(confData->get(configOption->GetKey()));
    if (str == nullptr) {
        return returnDefaultValue(configOption);
    }
    auto result = configOption->GetDefaultValue()->clone();
    result->setValue(str->getData());
    return result;
}

Object* Configuration::returnDefaultValue(ConfigOption* configOption)
{
    if (configOption->hasDefaultValue()) {
        return configOption->GetDefaultValue()->clone();
    }
    return nullptr;
}

void Configuration::setConfiguration(nlohmann::json config)
{
    for (auto& [keyS, valueS] : config.items()) {
        std::string key = keyS;
        std::string value = valueS;
        if (value == "") {
            continue;
        }
        confData->put(new String(key), new String(value));
    }
}

Object* Configuration::getValue(const std::unique_ptr<ConfigOption>& configOption)
{
    return getValue(configOption.get());
}
