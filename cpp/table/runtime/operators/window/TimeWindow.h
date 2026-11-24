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

#pragma once

#include <cstdint>
#include <iostream>
#include <cmath>

#include "core/typeinfo/typeconstants.h"
#include "core/typeutils/TypeSerializerSingleton.h"
#include "Window.h"
#include "core/memory/DataInputView.h"
#include "core/utils/MathUtils.h"

class TimeWindow : public Window {
public:
    TimeWindow();

    TimeWindow(int64_t start, int64_t end);

    int64_t getStart() const
    {
        return start;
    }

    int64_t getEnd() const
    {
        return end;
    }

    int64_t maxTimestamp() const override
    {
        return end - 1;
    }

    bool intersects(const TimeWindow& other) const
    {
        return this->start <= other.end && this->end >= other.start;
    }

    TimeWindow cover(const TimeWindow& other) const
    {
        return {std::min(this->start, other.start), std::max(this->end, other.end)};
    }

    static int64_t getWindowStartWithOffset(int64_t timestamp, int64_t offset, int64_t windowSize)
    {
        if (windowSize <= 0) {
            THROW_RUNTIME_ERROR("windowSize should be larger than 0.");
        }
        int64_t remainder = (timestamp - offset) % windowSize;
        return remainder < 0L ? timestamp - (remainder + windowSize) : timestamp - remainder;
    }

    size_t hashCode() const
    {
        int32_t endTransformed = static_cast<int32_t>((end << 1) + 1);
        return static_cast<size_t>(start + modInverse(endTransformed));
    }

    bool operator<(const TimeWindow& other) const
    {
        if (start == other.start) {
            return end < other.end;
        }
        return start < other.start;
    }

    bool operator>(const TimeWindow& other) const
    {
        if (start == other.start) {
            return end > other.end;
        }
        return start > other.start;
    }

    bool operator==(const TimeWindow& other) const
    {
        return start == other.start && end == other.end;
    }

    friend std::ostream& operator<<(std::ostream& os, const TimeWindow& obj)
    {
        os << "TimeWindow{start=" << std::to_string(obj.start) << ", end=" << std::to_string(obj.end) << '}';
        return os;
    }

    class Serializer : public TypeSerializerSingleton {
    public:
        static constexpr BackendDataType SERIALIZER_BACKEND_DATA_TYPE = BackendDataType::TIME_WINDOW_BK;
        static constexpr const char* SERIALIZER_NAME = "TimeWindow.Serializer";

        Serializer();

        bool isImmutableType() const;

        TimeWindow* createInstance() const;

        TimeWindow* copy(TimeWindow* from) const;

        TimeWindow* copy(TimeWindow* from, TimeWindow* reuse) const;

        int32_t getLength() const;

        void serialize(void* record, DataOutputSerializer& target) override;

        void* deserialize(DataInputView& source) override;

        void copy(DataInputView* source, DataOutputSerializer* target) const;

        BackendDataType getBackendId() const override
        {
            return SERIALIZER_BACKEND_DATA_TYPE;
        }

        const char* getName() const override
        {
            return SERIALIZER_NAME;
        }

        std::string toJson() override
        {
            SerializerJsonInfo typeJson = {SerializerType::TIME_WINDOW, TYPE_NAME_TIME_WINDOW_CLASS};
            return typeJson.toJson();
        }
    };

private:
    int64_t start{};
    int64_t end{};

    int32_t modInverse(int32_t x) const
    {
        uint32_t ux = static_cast<uint32_t>(x);
        // Cube gives inverse mod 2^4, as x^4 == 1 (mod 2^4) for all odd x.
        uint32_t inverse = ux * ux * ux;
        // Newton iteration doubles correct bits at each step.
        inverse *= 2 - ux * inverse;
        inverse *= 2 - ux * inverse;
        inverse *= 2 - ux * inverse;
        return static_cast<int32_t>(inverse);
    }
};

namespace std {
template <>
struct hash<TimeWindow> {
    std::size_t operator()(const TimeWindow& timeWindow) const noexcept
    {
        return timeWindow.hashCode();
    }
};
} // namespace std
