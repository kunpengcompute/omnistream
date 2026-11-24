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

#ifndef ENDOFDATA_H
#define ENDOFDATA_H

#include "RuntimeEvent.h"
#include "io/network/api/StopMode.h"

namespace omnistream {
class EndOfData : public RuntimeEvent {
public:
    explicit EndOfData(StopMode mode) : mode_(mode)
    {
    }
    StopMode getStopMode() const
    {
        return mode_;
    }

    int GetEventClassID() override
    {
        return AbstractEvent::endOfData;
    }

    std::string GetEventClassName() override
    {
        return "EndOfData";
    }
    // Comparison and hash
    bool Equals(const RuntimeEvent* other) const
    {
        if (this == other) {
            return true;
        }
        auto* eod = dynamic_cast<const EndOfData*>(other);
        return eod != nullptr && mode_ == eod->mode_;
    }

    size_t hashCode() const
    {
        return std::hash<StopMode>{}(mode_);
    }

    std::string toString() const
    {
        return "EndOfData{mode=" + std::to_string(static_cast<int>(mode_)) + "}";
    }
    // Optionally add operator== if direct comparisons are needed
    bool operator==(const EndOfData& rhs) const
    {
        return mode_ == rhs.mode_;
    }

private:
    StopMode mode_;
};
} // namespace omnistream

#endif // ENDOFDATA_H
