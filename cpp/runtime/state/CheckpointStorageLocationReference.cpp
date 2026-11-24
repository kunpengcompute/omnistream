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

#include "CheckpointStorageLocationReference.h"
#include "nlohmann/json.hpp"
#include "common.h"

CheckpointStorageLocationReference::CheckpointStorageLocationReference(
    std::shared_ptr<std::vector<uint8_t>> encodedReference)
    : encodedReference_(encodedReference)
{
    if (encodedReference == nullptr || encodedReference->empty()) {
        throw std::invalid_argument("encodedReference must not be empty");
    }
}

CheckpointStorageLocationReference::CheckpointStorageLocationReference() : encodedReference_(nullptr)
{
}

CheckpointStorageLocationReference::~CheckpointStorageLocationReference() = default;

std::shared_ptr<std::vector<uint8_t>> CheckpointStorageLocationReference::GetReferenceBytes() const
{
    return encodedReference_ != nullptr ? encodedReference_ : std::make_shared<std::vector<uint8_t>>();
}

bool CheckpointStorageLocationReference::IsDefaultReference() const
{
    return encodedReference_ == nullptr;
}

int CheckpointStorageLocationReference::HashCode() const
{
    if (this->encodedReference_ == nullptr) {
        return 2059243550;
    } else {
        int result = 1;
        auto var2 = this->encodedReference_;
        size_t var3 = this->encodedReference_->size();

        for (size_t var4 = 0; var4 < var3; ++var4) {
            uint8_t element = var2->at(var4);
            result = 31 * result + element;
        }
        return result;
    }
}

std::string CheckpointStorageLocationReference::ToString() const
{
    nlohmann::json json;
    if (encodedReference_ == nullptr) {
        json["referenceBytes"] = "(default)";
    } else {
        std::ostringstream oss;
        oss << std::hex << std::setfill('0');
        for (uint8_t byte : *encodedReference_) {
            oss << std::setw(2) << static_cast<int>(byte);
        }
        json["referenceBytes"] = oss.str();
    }
    return json.dump();
}

nlohmann::json CheckpointStorageLocationReference::ToJson() const
{
    nlohmann::json json;
    if (encodedReference_ == nullptr) {
        json["referenceBytes"] = "(default)";
    } else {
        json["referenceBytes"] =
            std::string(reinterpret_cast<const char*>(encodedReference_->data()), encodedReference_->size());
    }
    return json;
}

std::shared_ptr<CheckpointStorageLocationReference> CheckpointStorageLocationReference::DEFAULT =
    std::make_shared<CheckpointStorageLocationReference>();
