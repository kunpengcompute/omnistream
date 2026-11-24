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

#ifndef FLINK_TNEL_CHECKPOINTSTORAGELOCATIONREFERENCE_H
#define FLINK_TNEL_CHECKPOINTSTORAGELOCATIONREFERENCE_H

#include <iomanip>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>
#include <memory>
#include "nlohmann/json.hpp"

class CheckpointStorageLocationReference {
public:
    explicit CheckpointStorageLocationReference(std::shared_ptr<std::vector<uint8_t>> encodedReference);
    CheckpointStorageLocationReference();

    bool operator==(const CheckpointStorageLocationReference& other) const
    {
        if (!this->encodedReference_ && !other.encodedReference_) {
            return true;
        }

        if (!this->encodedReference_ || !other.encodedReference_) {
            return false;
        }
        return *(this->encodedReference_) == *(other.encodedReference_);
    }

    ~CheckpointStorageLocationReference();

    int HashCode() const;
    std::shared_ptr<std::vector<uint8_t>> GetReferenceBytes() const;
    bool IsDefaultReference() const;
    static std::shared_ptr<CheckpointStorageLocationReference> DEFAULT;
    static std::shared_ptr<CheckpointStorageLocationReference> GetDefault()
    {
        return DEFAULT;
    }
    std::string ToString() const;
    nlohmann::json ToJson() const;

private:
    std::shared_ptr<std::vector<uint8_t>> encodedReference_;
};

namespace std {
template <>
struct hash<CheckpointStorageLocationReference> {
    std::size_t operator()(const CheckpointStorageLocationReference& ref) const
    {
        return static_cast<std::size_t>(ref.HashCode());
    }
};
} // namespace std

#endif // FLINK_TNEL_CHECKPOINTSTORAGELOCATIONREFERENCE_H
