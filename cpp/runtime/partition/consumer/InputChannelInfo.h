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

// InputChannelInfo.h
#ifndef INPUTCHANNELINFO_H
#define INPUTCHANNELINFO_H

#include <string>

namespace omnistream {

class InputChannelInfo {
public:
    InputChannelInfo();
    InputChannelInfo(int gateIdx, int inputChannelIdx);
    InputChannelInfo(const InputChannelInfo& other);
    InputChannelInfo& operator=(const InputChannelInfo& other);
    ~InputChannelInfo();

    int getGateIdx() const;
    void setGateIdx(int gateIdx);
    void setOmni();
    bool getOmni();

    int getInputChannelIdx() const;
    void setInputChannelIdx(int inputChannelIdx);

    bool operator==(const InputChannelInfo& other) const;
    bool operator!=(const InputChannelInfo& other) const;
    bool operator<(const InputChannelInfo& other) const;

    std::string toString() const;

    int hashcode() const
    {
        return (gateIdx << 16) ^ inputChannelIdx;
    }

    bool equals(const InputChannelInfo& other) const
    {
        return gateIdx == other.gateIdx && inputChannelIdx == other.inputChannelIdx;
    }

    long getComplexId() const
    {
        return ((long)gateIdx) << 32 | inputChannelIdx;
    }

private:
    int gateIdx;
    int inputChannelIdx;
    // if the InputChannel is an OmniLocalInputChannel?
    bool omni = false;
};
struct InputChannelInfoHash {
    std::size_t operator()(const InputChannelInfo& own) const
    {
        return own.hashcode();
    }
};

struct InputChannelInfoEqual {
    bool operator()(const InputChannelInfo& lhs, const InputChannelInfo& rhs) const
    {
        if (lhs == rhs) return true;
        return lhs.equals(rhs);
    }
};

} // namespace omnistream

namespace std {
template <>
struct hash<omnistream::InputChannelInfo> {
    size_t operator()(const omnistream::InputChannelInfo& info) const noexcept
    {
        static constexpr size_t kHashCombineConstant = 0x9e3779b9;
        static constexpr int kLeftShift = 6;
        static constexpr int kRightShift = 2;

        size_t h1 = std::hash<int>{}(info.getGateIdx());
        size_t h2 = std::hash<int>{}(info.getInputChannelIdx());

        return h1 ^ (h2 + kHashCombineConstant + (h1 << kLeftShift) + (h1 >> kRightShift));
    }
};
} // namespace std

#endif // INPUTCHANNELINFO_H
