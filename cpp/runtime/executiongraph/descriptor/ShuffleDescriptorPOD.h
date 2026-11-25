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

#ifndef OMNISTREAM_SHUFFLE_DESCRIPTOR_POD_H
#define OMNISTREAM_SHUFFLE_DESCRIPTOR_POD_H

#include <string>
#include <iostream>
#include "ResultPartitionIDPOD.h" // Assuming this header exists
#include "ResourceIDPOD.h"        // Assuming this header exists
#include "nlohmann/json.hpp"      // Include for JSON serialization

namespace omnistream {

using json = nlohmann::json;

class ShuffleDescriptorPOD {
public:
    // Default constructor
    ShuffleDescriptorPOD() : unknown(false), hasLocalResource(false)
    {
    }

    // Full argument constructor
    ShuffleDescriptorPOD(
        const ResultPartitionIDPOD& resultPartitionID,
        bool isUnknown,
        bool hasLocalResource,
        const ResourceIDPOD& storesLocalResourcesOn)
        : resultPartitionID(resultPartitionID),
          unknown(isUnknown),
          hasLocalResource(hasLocalResource),
          storesLocalResourcesOn(storesLocalResourcesOn)
    {
    }

    friend bool operator==(const ShuffleDescriptorPOD& lhs, const ShuffleDescriptorPOD& rhs)
    {
        return lhs.resultPartitionID == rhs.resultPartitionID && lhs.unknown == rhs.unknown &&
               lhs.hasLocalResource == rhs.hasLocalResource && lhs.storesLocalResourcesOn == rhs.storesLocalResourcesOn;
    }

    friend bool operator!=(const ShuffleDescriptorPOD& lhs, const ShuffleDescriptorPOD& rhs)
    {
        return !(lhs == rhs);
    }

    friend std::size_t hash_value(const ShuffleDescriptorPOD& obj)
    {
        std::size_t seed = 0x0C583894;
        seed ^= (seed << 6) + (seed >> 2) + 0x58FAECDB + hash_value(obj.resultPartitionID);
        seed ^= (seed << 6) + (seed >> 2) + 0x223CE38E + static_cast<std::size_t>(obj.unknown);
        seed ^= (seed << 6) + (seed >> 2) + 0x5E1A1036 + static_cast<std::size_t>(obj.hasLocalResource);
        seed ^= (seed << 6) + (seed >> 2) + 0x05988364 + hash_value(obj.storesLocalResourcesOn);
        return seed;
    }

    // Getters
    const ResultPartitionIDPOD& getResultPartitionID() const
    {
        return resultPartitionID;
    }
    bool getIsUnknown() const
    {
        return unknown;
    }
    bool getHasLocalResource() const
    {
        return hasLocalResource;
    }
    const ResourceIDPOD& getStoresLocalResourcesOn() const
    {
        return storesLocalResourcesOn;
    }

    // Setters
    void setResultPartitionID(const ResultPartitionIDPOD& resultPartitionID_)
    {
        this->resultPartitionID = resultPartitionID_;
    }

    void setIsUnknown(bool isUnknown)
    {
        this->unknown = isUnknown;
    }
    void setHasLocalResource(bool hasLocalResource_)
    {
        this->hasLocalResource = hasLocalResource_;
    }

    void setStoresLocalResourcesOn(const ResourceIDPOD& storesLocalResourcesOn_)
    {
        this->storesLocalResourcesOn = storesLocalResourcesOn_;
    }

    // toString method
    std::string toString() const
    {
        return "ShuffleDescriptorPOD{"
               "resultPartitionID=" +
               resultPartitionID.toString() + // Assuming ResultPartitionIDPOD has a toString()
               ", isUnknown=" + std::to_string(unknown) + ", hasLocalResource=" + std::to_string(hasLocalResource) +
               ", storesLocalResourcesOn=" +
               storesLocalResourcesOn.toString() + // Assuming ResourceIDPOD has a toString()
               "}";
    }

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(
        ShuffleDescriptorPOD, resultPartitionID, unknown, hasLocalResource, storesLocalResourcesOn)

private:
    ResultPartitionIDPOD resultPartitionID; // Class: ResultPartitionIDPOD, Variable: resultPartitionID
    bool unknown;                           // Class: bool, Variable: isUnknown
    bool hasLocalResource;                  // Class: bool, Variable: hasLocalResource
    ResourceIDPOD storesLocalResourcesOn;   // Class: ResourceIDPOD, Variable: storesLocalResourcesOn
};
} // namespace omnistream

namespace std {
template <>
struct hash<omnistream::ShuffleDescriptorPOD> {
    std::size_t operator()(const omnistream::ShuffleDescriptorPOD& obj) const
    {
        return hash_value(obj);
    }
};
} // namespace std

#endif // OMNISTREAM_SHUFFLE_DESCRIPTOR_POD_H
