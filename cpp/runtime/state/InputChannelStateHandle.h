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

#ifndef OMNISTREAM_INPUTCHANNELSTATEHANDLE_H
#define OMNISTREAM_INPUTCHANNELSTATEHANDLE_H
#include "runtime/state/StateObject.h"
#include "AbstractChannelStateHandle.h"
#include "core/include/common.h"
#include "runtime/partition/consumer/InputChannelInfo.h"
class InputChannelStateHandle : public omnistream::AbstractChannelStateHandle<omnistream::InputChannelInfo> {
public:
    InputChannelStateHandle(
        int subTaskIndex,
        omnistream::InputChannelInfo info,
        std::shared_ptr<StreamStateHandle> delegate,
        StateContentMetaInfo contentMetaInfo)
        : omnistream::AbstractChannelStateHandle<omnistream::InputChannelInfo>(
              subTaskIndex, info, delegate, contentMetaInfo.GetOffsets(), contentMetaInfo.GetSize())
    {
    }
    // This class should be a template with InputChannelInfo, and info should be of class InputChannelInfo
    // This is a temporary place holder for checkpointing's PioritizedOperatorSubtaskState

    void DiscardState() override
    {
    }

    std::string ToString() const override
    {
        nlohmann::json j;
        nlohmann::json state_offset_vec = nlohmann::json::array();
        for (const auto& offset : offsets) {
            state_offset_vec.push_back(offset);
        }
        nlohmann::json state_offset_array = nlohmann::json::array();
        state_offset_array.push_back("java.util.Collections$UnmodifiableRandomAccessList");
        state_offset_array.push_back(state_offset_vec);
        j["@class"] = "InputChannelStateHandle";
        j["info"] = nlohmann::json::parse(info.toString());
        j["delegate"] = nlohmann::json::parse(delegate->ToString());
        j["offsets"] = state_offset_array;
        j["size"] = size;
        j["subtaskIndex"] = GetSubtaskIndex();
        j["stateSize"] = size;
        return j.dump();
    };
};
#endif // OMNISTREAM_INPUTCHANNELSTATEHANDLE_H
