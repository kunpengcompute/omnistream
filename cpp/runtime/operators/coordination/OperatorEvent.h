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

#ifndef FLINK_TNEL_OPERATOREVENT_H
#define FLINK_TNEL_OPERATOREVENT_H

#include "io/SimpleVersionedSerializer.h"

class OperatorEvent {
public:
    virtual ~OperatorEvent() = default;
    virtual std::string toString() = 0;
};

class WatermarkAlignmentEvent : public OperatorEvent {
public:
    WatermarkAlignmentEvent(long maxWatermark) : maxWatermark(maxWatermark)
    {
    }

    long getMaxWatermark()
    {
        return maxWatermark;
    }
    std::string toString() override
    {
        nlohmann::json j;
        j["maxWatermark"] = maxWatermark;
        return j.dump();
    }

private:
    long maxWatermark;
};

template <typename E>
class AddSplitEvent : public OperatorEvent {
public:
    AddSplitEvent(int serializerVersion, std::vector<std::vector<uint8_t>> splitsVec)
        : serializerVersion(serializerVersion),
          splitsVec(std::move(splitsVec))
    {
    }

    AddSplitEvent(std::vector<E> splits, SimpleVersionedSerializer<E>* splitSerializer)
    {
        splitsVec.reserve(splits.size());
        serializerVersion = splitSerializer->getVersion();
        for (const auto& split : splits) {
            splitsVec.push_back(splitSerializer->serialize(split));
        }
    }

    std::vector<E*> splits(SimpleVersionedSerializer<E>* splitSerializer) const
    {
        std::vector<E*> result;
        result.reserve(splitsVec.size());
        // 遍历所有序列化后的分片数据
        for (auto serializedSplit : splitsVec) {
            // 调用反序列化方法并将结果添加到结果向量中
            result.push_back(splitSerializer->deserialize(serializerVersion, serializedSplit));
        }

        return result;
    }
    std::string toString() override
    {
        nlohmann::json j;
        j["event"] = "AddSplitEvent";
        j["serializerVersion"] = serializerVersion;
        return j.dump();
    }

private:
    int serializerVersion;
    std::vector<std::vector<uint8_t>> splitsVec;
};

class SourceEvent {
public:
    virtual ~SourceEvent() = default;
};

class NoMoreSplitsEvent : public OperatorEvent {
    std::string toString() override
    {
        nlohmann::json j;
        j["event"] = "NoMoreSplitsEvent";
        return j.dump();
    }
};

#endif // FLINK_TNEL_OPERATOREVENT_H
