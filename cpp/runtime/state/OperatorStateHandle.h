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

#ifndef OMNISTREAM_OPERATORSTATEHANDLE_H
#define OMNISTREAM_OPERATORSTATEHANDLE_H

#include <vector>
#include <unordered_map>
#include "core/fs/FSDataInputStream.h"
#include "StreamStateHandle.h"

class OperatorStateHandle : public StreamStateHandle {
public:
    enum class Mode {
        SPLIT_DISTRIBUTE,
        UNION,
        BROADCAST
    };

    static Mode StrToMode(const std::string& mode)
    {
        static std::unordered_map<std::string, Mode> strToMode = {
            {"SPLIT_DISTRIBUTE", Mode::SPLIT_DISTRIBUTE}, {"UNION", Mode::UNION}, {"BROADCAST", Mode::BROADCAST}};
        if (strToMode.find(mode) != strToMode.end()) {
            return strToMode[mode];
        }

        throw std::invalid_argument("Unknown OperatorStateHandle::Mode : " + mode);
    }

    class StateMetaInfo {
    public:
        StateMetaInfo() = default;
        StateMetaInfo(std::vector<long> offsets, OperatorStateHandle::Mode distributionMode);
        std::vector<long> getOffsets() const;
        OperatorStateHandle::Mode getDistributionMode() const;

        bool operator==(const StateMetaInfo& other) const;

    private:
        std::vector<long> offsets_;
        OperatorStateHandle::Mode distributionMode_;
    };

    virtual std::unordered_map<std::string, StateMetaInfo> getStateNameToPartitionOffsets() const = 0;
    virtual std::shared_ptr<StreamStateHandle> getDelegateStateHandle() const = 0;
};
#endif // OMNISTREAM_OPERATORSTATEHANDLE_H
