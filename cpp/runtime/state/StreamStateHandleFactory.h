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

#ifndef OMNISTREAM_STREAMSTATEHANDLEFACTORY_H
#define OMNISTREAM_STREAMSTATEHANDLEFACTORY_H

#include <memory>
#include "runtime/state/StreamStateHandle.h"
#include "runtime/state/filesystem/FileStateHandle.h"
#include "state/filesystem/RelativeFileStateHandle.h"
#include "state/memory/ByteStreamStateHandle.h"
#include "PlaceholderStreamStateHandle.h"
#include <nlohmann/json.hpp>
#include <stdexcept>

class StreamStateHandleFactory {
public:
    static std::shared_ptr<StreamStateHandle> from_json(const nlohmann::json& json)
    {
        if (!json.is_object()) {
            throw std::runtime_error(
                "metadata State JSON must be an object, actual type: " + std::string(json.type_name()));
        }
        const std::string className = GetClassName(json);
        if (className.find("RelativeFileStateHandle") != std::string::npos) {
            Path filePath(GetFilePath(json));
            long stateSize = GetLong(json, "stateSize");
            std::string relativePath = GetString(json, "relativePath");
            return std::make_shared<RelativeFileStateHandle>(filePath, relativePath, stateSize);
        } else if (className.find("FileStateHandle") != std::string::npos) {
            long stateSize = GetLong(json, "stateSize");
            Path filePath(GetFilePath(json));
            return std::make_shared<FileStateHandle>(filePath, stateSize);
        } else if (className.find("ByteStreamStateHandle") != std::string::npos) {
            std::string handleName = GetString(json, "handleName");
            if (!json.contains("data") || !json.at("data").is_string()) {
                throw std::runtime_error("ByteStreamStateHandle JSON is missing string field 'data'.");
            }
            std::string encoded_data = json.at("data").get<std::string>();
            return std::make_shared<ByteStreamStateHandle>(handleName, Base64_decode(encoded_data));
        } else if (className.find("PlaceholderStreamStateHandle") != std::string::npos) {
            long stateSize = GetLong(json, "stateSize");
            std::string keyString = GetNestedString(json, "physicalID", "keyString");
            std::unique_ptr<PhysicalStateHandleID> physicalID = std::make_unique<PhysicalStateHandleID>(keyString);
            std::shared_ptr<PlaceholderStreamStateHandle> handle =
                std::make_shared<PlaceholderStreamStateHandle>(std::move(physicalID), stateSize);
            return handle;
        }
        return nullptr;
    }

private:
    static std::string GetClassName(const nlohmann::json& json)
    {
        if (json.contains("@class") && json.at("@class").is_string()) {
            return json.at("@class").get<std::string>();
        }
        if (json.contains("stateHandleName") && json.at("stateHandleName").is_string()) {
            return json.at("stateHandleName").get<std::string>();
        }
        throw std::runtime_error("metadata State JSON is missing '@class' or 'stateHandleName'.");
    }

    static std::string GetString(const nlohmann::json& json, const std::string& field)
    {
        if (json.contains(field) && json.at(field).is_string()) {
            return json.at(field).get<std::string>();
        }
        throw std::runtime_error("StreamStateHandle JSON is missing string field '" + field + "'.");
    }

    static long GetLong(const nlohmann::json& json, const std::string& field)
    {
        if (json.contains(field) && json.at(field).is_number_integer()) {
            return json.at(field).get<long>();
        }
        throw std::runtime_error("StreamStateHandle JSON is missing integer field '" + field + "'.");
    }

    static std::string GetNestedString(
        const nlohmann::json& json, const std::string& objectField, const std::string& stringField)
    {
        if (!json.contains(objectField) || !json.at(objectField).is_object()) {
            throw std::runtime_error("StreamStateHandle JSON is missing object field '" + objectField + "'.");
        }
        const auto& nested = json.at(objectField);
        if (nested.contains(stringField) && nested.at(stringField).is_string()) {
            return nested.at(stringField).get<std::string>();
        }
        throw std::runtime_error(
            "StreamStateHandle JSON is missing string field '" + objectField + "." + stringField + "'.");
    }

    static std::string GetFilePath(const nlohmann::json& json)
    {
        if (json.contains("filePath") && json.at("filePath").is_string()) {
            return json.at("filePath").get<std::string>();
        }
        return GetNestedString(json, "streamStateHandleID", "keyString");
    }
};

#endif // OMNISTREAM_STREAMSTATEHANDLEFACTORY_H
