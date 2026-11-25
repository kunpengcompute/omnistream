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

#include "thirdlibrary/java_util_Properties.h"
#include <iostream>
#include <fstream>

java_util_Properties::java_util_Properties() = default;
java_util_Properties::~java_util_Properties() = default;

int lib_fun()
{
    return 0;
}

void java_util_Properties::load(InputStream* input)
{
    Dl_info dlInfo{};
    if (dladdr(reinterpret_cast<void*>(lib_fun), &dlInfo) == 0 || dlInfo.dli_fname == nullptr) {
        std::cerr << "Failed to resolve the library path for flinkMetrics.properties" << std::endl;
        return;
    }

    const std::string libraryPath(dlInfo.dli_fname);
    const size_t separator = libraryPath.find_last_of('/');
    // 未包含目录分隔符时保持原有行为，从当前工作目录读取配置文件
    const std::string directory = separator == std::string::npos ? "" : libraryPath.substr(0, separator + 1);
    const std::string filePath = directory + "flinkMetrics.properties";

    std::ifstream file(filePath, std::ios::in);

    if (!file.is_open()) {
        return;
    }

    std::string strLine;
    while (std::getline(file, strLine)) {
        processLine(strLine);
    }
}

void java_util_Properties::processLine(std::string& strLine)
{
    if (strLine.empty() || strLine[0] == '#' || strLine[0] == '!') {
        return;
    }
    if (strLine.back() == '\r') {
        strLine.erase(strLine.size() - 1);
    }

    std::string key;
    std::string value;
    parseKeyValueFromLine(strLine, key, value);

    if (!key.empty()) {
        this->put(key, value);
    }
}

void java_util_Properties::parseKeyValueFromLine(std::string& strLine, std::string& outKey, std::string& outValue)
{
    size_t limit = strLine.size();
    size_t keyLen = 0;
    size_t valueStart = 0;
    bool hasSep = false;
    bool precedingBackslash = false;
    char c = 0;

    while (keyLen < limit) {
        c = strLine[keyLen];
        if ((c == '=' || c == ':') && !precedingBackslash) {
            valueStart = keyLen + 1;
            hasSep = true;
            break;
        } else if ((c == ' ' || c == '\t' || c == '\f') && !precedingBackslash) {
            valueStart = keyLen + 1;
            break;
        }
        if (c == '\\') {
            precedingBackslash = !precedingBackslash;
        } else {
            precedingBackslash = false;
        }
        keyLen++;
    }

    while (valueStart < limit) {
        c = strLine[valueStart];
        if (c != ' ' && c != '\t' && c != '\f') {
            if (!hasSep && (c == '=' || c == ':')) {
                hasSep = true;
            } else {
                break;
            }
        }
        valueStart++;
    }
    outKey = strLine.substr(0, keyLen);
    outValue = strLine.substr(valueStart, limit - valueStart);
}
