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

#ifndef OMNISTREAM_INFLIGHTDATARESCALINGDESCRIPTOR_H
#define OMNISTREAM_INFLIGHTDATARESCALINGDESCRIPTOR_H

#include <vector>
#include <memory>
#include <stdexcept>
#include <algorithm>
#include <sstream>
#include <typeinfo>
#include <functional>
#include <set>
#include <string>
#include <iostream>
#include "runtime/checkpoint/RescaleMappings.h"
namespace omnistream {
class InflightDataRescalingDescriptor;

class InflightDataGateOrPartitionRescalingDescriptor {
    friend class InflightDataRescalingDescriptor;

public:
    /** Type of mapping which should be used for this in-flight data. */
    enum class MappingType {
        IDENTITY,
        RESCALING
    };

    InflightDataGateOrPartitionRescalingDescriptor(
        const std::vector<int>& oldSubtaskIndexes,
        const std::shared_ptr<RescaleMappings>& rescaledChannelsMappings,
        const std::set<int>& ambiguousSubtaskIndexes,
        MappingType mappingType) noexcept
        : oldSubtaskIndexes(oldSubtaskIndexes),
          rescaledChannelsMappings(rescaledChannelsMappings),
          ambiguousSubtaskIndexes(ambiguousSubtaskIndexes),
          mappingType(mappingType)
    {
    }

    bool IsIdentity() const
    {
        return mappingType == MappingType::IDENTITY;
    }

    bool operator==(const InflightDataGateOrPartitionRescalingDescriptor& o) const;

    bool operator!=(const InflightDataGateOrPartitionRescalingDescriptor& o) const
    {
        return !(*this == o);
    }

    std::size_t HashCode() const;

    std::string ToString() const;

    // Getter methods for accessing private members (C++ best practice)
    const std::vector<int>& GetOldSubtaskIndexes() const
    {
        return oldSubtaskIndexes;
    }

    const std::shared_ptr<RescaleMappings> GetRescaledChannelsMappings() const
    {
        return rescaledChannelsMappings;
    }

    const std::set<int>& GetAmbiguousSubtaskIndexes() const
    {
        return ambiguousSubtaskIndexes;
    }

    MappingType GetMappingType() const
    {
        return mappingType;
    }

private:
    static const long serialVersionUID = 1L;
    /** Set when several operator instances are merged into one. */
    const std::vector<int> oldSubtaskIndexes;
    /**
     * Set when channels are merged because the connected operator has been rescaled for each
     * gate/partition.
     */
    const std::shared_ptr<RescaleMappings> rescaledChannelsMappings;
    /** All channels where upstream duplicates data (only valid for downstream mappings). */
    const std::set<int> ambiguousSubtaskIndexes;
    const MappingType mappingType;
};

class InflightDataRescalingDescriptor {
public:
    static std::shared_ptr<InflightDataRescalingDescriptor> noRescale;

    explicit InflightDataRescalingDescriptor(
        const std::vector<InflightDataGateOrPartitionRescalingDescriptor>& gateOrPartitionDescriptors) noexcept
        : gateOrPartitionDescriptors(gateOrPartitionDescriptors)
    {
    }

    virtual std::vector<int> GetOldSubtaskIndexes(int gateOrPartitionIndex) const
    {
        return gateOrPartitionDescriptors[gateOrPartitionIndex].oldSubtaskIndexes;
    }

    virtual std::shared_ptr<RescaleMappings> GetChannelMapping(int gateOrPartitionIndex) const
    {
        return gateOrPartitionDescriptors[gateOrPartitionIndex].rescaledChannelsMappings;
    }

    virtual bool IsAmbiguous(int gateOrPartitionIndex, int oldSubtaskIndex) const
    {
        const auto& ambiguousIndexes = gateOrPartitionDescriptors[gateOrPartitionIndex].ambiguousSubtaskIndexes;
        return std::find(ambiguousIndexes.begin(), ambiguousIndexes.end(), oldSubtaskIndex) != ambiguousIndexes.end();
    }

    bool Equals(const InflightDataRescalingDescriptor* o) const
    {
        if (this == o) {
            return true;
        }
        if (o == nullptr || typeid(*this) != typeid(*o)) {
            return false;
        }
        return gateOrPartitionDescriptors == o->gateOrPartitionDescriptors;
    }

    bool operator==(const InflightDataRescalingDescriptor& other) const
    {
        return Equals(&other);
    }

    std::string ToString() const
    {
        std::ostringstream oss;
        oss << "InflightDataRescalingDescriptor{";
        oss << "gateOrPartitionDescriptors=[";
        for (size_t i = 0; i < gateOrPartitionDescriptors.size(); ++i) {
            if (i > 0) {
                oss << ", ";
            }
            oss << gateOrPartitionDescriptors[i].ToString();
        }
        oss << "]}";
        return oss.str();
    }

    std::vector<InflightDataGateOrPartitionRescalingDescriptor> GetGateOrPartitionDescriptors()
    {
        return gateOrPartitionDescriptors;
    }

protected:
    // Default constructor for subclasses
    InflightDataRescalingDescriptor() = default;

private:
    static const long serialVersionUID = -3396674344669796295L;

    /** Set when several operator instances are merged into one. */
    std::vector<InflightDataGateOrPartitionRescalingDescriptor> gateOrPartitionDescriptors;
};

// NoRescalingDescriptor class definition (referenced by noRescale)
class NoRescalingDescriptor : public InflightDataRescalingDescriptor {
public:
    NoRescalingDescriptor() noexcept
        : InflightDataRescalingDescriptor(std::vector<InflightDataGateOrPartitionRescalingDescriptor>())
    {
    }

    std::vector<int> GetOldSubtaskIndexes(int gateOrPartitionIndex) const override
    {
        return std::vector<int>();
    }

    std::shared_ptr<RescaleMappings> GetChannelMapping(int gateOrPartitionIndex) const override
    {
        return IdentityRescaleMappings::SYMMETRIC_IDENTITY;
    }

    bool IsAmbiguous(int gateOrPartitionIndex, int oldSubtaskIndex) const override
    {
        return false;
    }
};
} // namespace omnistream
#endif // OMNISTREAM_INFLIGHTDATARESCALINGDESCRIPTOR_H
