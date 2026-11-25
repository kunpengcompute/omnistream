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

#include "RescaleMappings.h"

namespace omnistream {

std::shared_ptr<RescaleMappings> RescaleMappings::SYMMETRIC_IDENTITY =
    std::make_shared<IdentityRescaleMappings>(std::numeric_limits<int>::max(), std::numeric_limits<int>::max());

RescaleMappings RescaleMappings::of(const std::vector<std::vector<int>>& mappedTargets, int numberOfTargets_)
{
    std::vector<std::vector<int>> mappings_;
    for (const auto& targets : mappedTargets) {
        mappings_.push_back(targets.empty() ? std::vector<int>{} : targets);
    }

    if (isIdentity(mappings_, numberOfTargets_)) {
        return IdentityRescaleMappings(mappings_.size(), numberOfTargets_);
    }

    size_t lastNonEmpty = mappings_.size() - 1;
    while (lastNonEmpty >= 0 && mappings_[lastNonEmpty].empty()) {
        --lastNonEmpty;
    }

    size_t length = lastNonEmpty + 1;
    return RescaleMappings(
        mappings_.size(),
        length == mappings_.size() ? mappings_
                                   : std::vector<std::vector<int>>(mappings_.begin(), mappings_.begin() + length),
        numberOfTargets_);
}
std::string RescaleMappings::ToString() const
{
    std::ostringstream oss;
    oss << "RescaleMappings{numberOfSources=" << numberOfSources << ", numberOfTargets=" << numberOfTargets;
    oss << ", mappings=[";
    for (size_t i = 0; i < mappings.size(); ++i) {
        if (i > 0) {
            oss << ", ";
        }
        oss << "[";
        for (size_t j = 0; j < mappings[i].size(); ++j) {
            if (j > 0) {
                oss << ", ";
            }
            oss << mappings[i][j];
        }
        oss << "]";
    }
    oss << "]}";
    return oss.str();
}

std::set<int> RescaleMappings::getAmbiguousTargets()
{
    std::set<int> ambiguousTargets;
    std::vector<bool> usedTargets(numberOfTargets, false);

    for (const auto& targets : mappings) {
        for (int target : targets) {
            if (usedTargets[target]) {
                ambiguousTargets.insert(target);
            } else {
                usedTargets[target] = true;
            }
        }
    }
    return ambiguousTargets;
}

bool RescaleMappings::isIdentity(const std::vector<std::vector<int>>& mappings_, size_t numberOfTargets_)
{
    if (mappings_.size() < numberOfTargets_) {
        return false;
    }
    for (auto source = numberOfTargets_; source < mappings_.size(); ++source) {
        if (!mappings_[source].empty()) {
            return false;
        }
    }
    for (int source = 0; source < static_cast<int>(numberOfTargets_); ++source) {
        if (mappings_[source].size() != 1 || source != mappings_[source][0]) {
            return false;
        }
    }
    return true;
}

RescaleMappings RescaleMappings::invert()
{
    std::vector<std::vector<int>> inverted(numberOfTargets);
    for (int source = 0; source < mappings.size(); ++source) {
        const auto& targets = mappings[source];
        for (int target : targets) {
            inverted[target].push_back(source);
        }
    }
    for (auto& sources : inverted) {
        std::sort(sources.begin(), sources.end());
    }
    return of(std::move(inverted), numberOfSources);
}

} // namespace omnistream
