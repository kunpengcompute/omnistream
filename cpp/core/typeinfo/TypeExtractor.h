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

#ifndef OMNISTREAM_TYPEEXTRACTOR_H
#define OMNISTREAM_TYPEEXTRACTOR_H

#include "basictypes/Class.h"
#include "TypeInformation.h"
#include "core/typeinfo/BasicTypeInfo.h"
#include "PojoField.h"
#include "PojoTypeInfo.h"
#include "MapTypeInfo.h"
#include "TimerTypeInfo.h"
#include "runtime/state/VoidNamespace.h"
#include "runtime/state/VoidNamespaceTypeInfo.h"
#include "basictypes/ClassRegistry.h"
#include "streaming/api/operators/TimerHeapInternalTimer.h"

class TypeExtractor {
public:
    inline static std::string MAP_NAME = "java_util_Map";

    inline static TypeInformation* CreateTypeInfo(Class* cl)
    {
        std::string className = cl->name;
        auto basicTypeInfo = BasicTypeInfo::getBasicTypeInfoByClass(className);
        if (basicTypeInfo != nullptr) {
            return basicTypeInfo;
        } else if (className.compare(0, MAP_NAME.length(), MAP_NAME) == 0) {
            // java_util_Map<java_lang_String,java_util_Map<java_lang_String,java_lang_String>>
            auto genericTypes = className.substr(MAP_NAME.length() + 1, className.length() - MAP_NAME.length() - 2);
            auto types = splitAndTrim(genericTypes);
            auto keyTypeInfo = CreateTypeInfo(ClassRegistry::instance().getClass(types.first));
            auto valueTypeInfo = CreateTypeInfo(ClassRegistry::instance().getClass(types.second));
            return new MapTypeInfo(keyTypeInfo, valueTypeInfo);
        } else if (
            strcasecmp(className.c_str(), TYPE_NAME_VOID_NAMESPACE) == 0 ||
            className == TYPE_NAME_VOID_NAMESPACE_CLASS || className == TYPE_NAME_VOID_NAMESPACE_CLASS_LINE) {
            return new VoidNamespaceTypeInfo(className.c_str());
        } else if (
            strcasecmp(className.c_str(), TYPE_NAME_TIMER) == 0 || className == TYPE_NAME_TIMER_CLASS ||
            className == TYPE_NAME_TIMER_CLASS_LINE) {
            auto timer = static_cast<TimerHeapInternalTimer<Object*, Object*>*>(cl->newInstance());
            Class* keyClazz = timer->getKey()->getClass();
            Class* namespaceClazz = timer->getNamespace()->getClass();
            auto keyTypeInfo = CreateTypeInfo(keyClazz);
            auto namespaceTypeInfo = CreateTypeInfo(namespaceClazz);
            return new TimerTypeInfo(keyTypeInfo, namespaceTypeInfo, keyClazz, namespaceClazz);
        }
        auto typeInformation = analyzePojo(cl);
        if (typeInformation != nullptr) {
            return typeInformation;
        }
        THROW_LOGIC_EXCEPTION("unsupported class info..");
    }

    inline static std::pair<std::string, std::string> splitAndTrim(const std::string& content)
    {
        size_t commaPos = content.find(',');
        if (commaPos == std::string::npos) {
            THROW_LOGIC_EXCEPTION("illegal format : A comma is missing");
        }

        std::string first = content.substr(0, commaPos);
        std::string second = content.substr(commaPos + 1);

        // 去除首尾空格
        auto trim = [](std::string& s) {
            s.erase(s.begin(), std::find_if(s.begin(), s.end(), [](int ch) { return !std::isspace(ch); }));
            s.erase(std::find_if(s.rbegin(), s.rend(), [](int ch) { return !std::isspace(ch); }).base(), s.end());
        };

        trim(first);
        trim(second);

        return {first, second};
    }

    inline static TypeInformation* analyzePojo(Class* cl)
    {
        if (cl->fieldTypes_.empty()) {
            return nullptr;
        }
        std::vector<PojoField*> pojoFields;
        for (auto& pair : cl->fieldTypes_) {
            auto pojoField = new PojoField(pair.first, CreateTypeInfo(ClassRegistry::instance().getClass(pair.second)));
            pojoFields.push_back(pojoField);
        }
        return new PojoTypeInfo(cl->name, pojoFields);
    }
};

#endif // OMNISTREAM_TYPEEXTRACTOR_H
