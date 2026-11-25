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

#include "InflightDataRescalingDescriptor.h"

namespace omnistream {
std::shared_ptr<InflightDataRescalingDescriptor> InflightDataRescalingDescriptor::noRescale =
    std::make_shared<NoRescalingDescriptor>();

bool InflightDataGateOrPartitionRescalingDescriptor::operator==(
    const InflightDataGateOrPartitionRescalingDescriptor& o) const
{
    if (this == &o) {
        return true;
    }
    const InflightDataGateOrPartitionRescalingDescriptor& that = o;
    return oldSubtaskIndexes == that.oldSubtaskIndexes && rescaledChannelsMappings == that.rescaledChannelsMappings &&
           ambiguousSubtaskIndexes == that.ambiguousSubtaskIndexes && mappingType == that.mappingType;
}

std::size_t InflightDataGateOrPartitionRescalingDescriptor::HashCode() const
{
    // This is not used
    return 0;
}

std::string InflightDataGateOrPartitionRescalingDescriptor::ToString() const
{
    std::ostringstream oss;
    oss << "InflightDataGateOrPartitionRescalingDescriptor{";
    oss << "oldSubtaskIndexes=[";

    // Equivalent to Arrays.toString(oldSubtaskIndexes)
    for (size_t i = 0; i < oldSubtaskIndexes.size(); ++i) {
        if (i > 0) {
            oss << ", ";
        }
        oss << oldSubtaskIndexes[i];
    }
    oss << "]";
    oss << ", rescaledChannelsMappings=" << rescaledChannelsMappings->ToString();
    oss << ", ambiguousSubtaskIndexes={";
    bool first = true;
    for (const auto& index : ambiguousSubtaskIndexes) {
        if (!first) {
            oss << ", ";
        }
        oss << index;
        first = false;
    }
    oss << "}";

    oss << ", mappingType=";
    switch (mappingType) {
        case MappingType::IDENTITY: oss << "IDENTITY"; break;
        case MappingType::RESCALING: oss << "RESCALING"; break;
    }
    oss << "}";
    return oss.str();
}
} // namespace omnistream
