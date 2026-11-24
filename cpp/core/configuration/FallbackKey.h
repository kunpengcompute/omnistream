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

#ifndef OMNISTREAM_FALLBACKKEY
#define OMNISTREAM_FALLBACKKEY

#include <string>

class FallbackKey {
public:
    static FallbackKey* createFallbackKey(std::string key)
    {
        return new FallbackKey(key, false);
    };
    static FallbackKey* createDeprecatedKey(std::string key)
    {
        return new FallbackKey(key, true);
    };

    std::string getKey() const
    {
        return key_;
    };

    bool isDepricated() const
    {
        return isDepricated_;
    };

    bool operator==(const FallbackKey& other) const
    {
        return key_ == other.key_ && isDepricated_ == other.isDepricated_;
    }

private:
    FallbackKey(std::string key, bool isDeprecated) : key_(key), isDepricated_(isDeprecated) {};
    std::string key_;
    bool isDepricated_;
};

namespace std {
template <>
struct hash<FallbackKey> {
    size_t operator()(const FallbackKey& record) const
    {
        return 31 * std::hash<std::string>{}(record.getKey()) + (record.isDepricated() ? 1 : 0);
    }
};
}; // namespace std

#endif // OMNISTREAM_FALLBACKKEY
