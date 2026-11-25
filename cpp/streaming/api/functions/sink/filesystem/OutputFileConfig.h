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

#ifndef OUTPUT_FILE_CONFIG_H
#define OUTPUT_FILE_CONFIG_H

#include <string>

class OutputFileConfigBuilder;

class OutputFileConfig {
public:
    OutputFileConfig(const std::string& partPrefix, const std::string& partSuffix)
        : partPrefix(partPrefix),
          partSuffix(partSuffix)
    {
    }

    const std::string& getPartPrefix() const
    {
        return partPrefix;
    }

    const std::string& getPartSuffix() const
    {
        return partSuffix;
    }

    static inline OutputFileConfigBuilder builder();

private:
    const std::string partPrefix;
    const std::string partSuffix;
};

class OutputFileConfigBuilder {
public:
    inline OutputFileConfigBuilder();

    inline OutputFileConfigBuilder& withPartPrefix(const std::string& prefix)
    {
        this->partPrefix = prefix;
        return *this;
    }

    inline OutputFileConfigBuilder& withPartSuffix(const std::string& suffix)
    {
        this->partSuffix = suffix;
        return *this;
    }

    inline OutputFileConfig build()
    {
        return OutputFileConfig(partPrefix, partSuffix);
    }

private:
    static inline const std::string DEFAULT_PART_PREFIX = "part";
    static inline const std::string DEFAULT_PART_SUFFIX = "";
    std::string partPrefix;
    std::string partSuffix;
};

inline OutputFileConfigBuilder::OutputFileConfigBuilder()
    : partPrefix(DEFAULT_PART_PREFIX),
      partSuffix(DEFAULT_PART_SUFFIX)
{
}

inline OutputFileConfigBuilder OutputFileConfig::builder()
{
    return OutputFileConfigBuilder();
}

#endif // OUTPUT_FILE_CONFIG_H
