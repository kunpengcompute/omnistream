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

#ifndef ORIGINALRECORD_H
#define ORIGINALRECORD_H

#include <nlohmann/json.hpp>
#include <string>
#include <sstream>

class OriginalRecord {
public:
    OriginalRecord()
        : key(""),
          left(false),
          leftTotalCount(0),
          rightTotalCount(0),
          currentLeftId(0),
          currentRightId(0),
          value(""),
          timestamp(0) {};

    OriginalRecord(
        const std::string& key,
        bool left,
        long leftTotalCount,
        long rightTotalCount,
        long currentLeftId,
        long currentRightId,
        const std::string& value,
        long timestamp)
        : key(key),
          left(left),
          leftTotalCount(leftTotalCount),
          rightTotalCount(rightTotalCount),
          currentLeftId(currentLeftId),
          currentRightId(currentRightId),
          value(value),
          timestamp(timestamp)
    {
    }

    const std::string& getKey() const
    {
        return key;
    }
    bool isLeft() const
    {
        return left;
    }
    long getLeftTotalCount() const
    {
        return leftTotalCount;
    }
    long getRightTotalCount() const
    {
        return rightTotalCount;
    }
    long getCurrentLeftId() const
    {
        return currentLeftId;
    }
    long getCurrentRightId() const
    {
        return currentRightId;
    }
    const std::string& getValue() const
    {
        return value;
    }
    long getTimestamp() const
    {
        return timestamp;
    }

    void setKey(const std::string& k)
    {
        key = k;
    }
    void setLeft(bool _left)
    {
        this->left = _left;
    }
    void setLeftTotalCount(long count)
    {
        leftTotalCount = count;
    }
    void setRightTotalCount(long count)
    {
        rightTotalCount = count;
    }
    void setCurrentLeftId(long id)
    {
        currentLeftId = id;
    }
    void setCurrentRightId(long id)
    {
        currentRightId = id;
    }
    void setValue(const std::string& v)
    {
        value = v;
    }
    void setTimestamp(long ts)
    {
        timestamp = ts;
    }

    bool operator<(const OriginalRecord& other) const
    {
        return timestamp < other.timestamp;
    }

    std::string toString() const
    {
        std::ostringstream oss;
        oss << "OriginalRecord{"
            << "key='" << key << "', "
            << "left=" << left << ", "
            << "leftTotalCount=" << leftTotalCount << ", "
            << "rightTotalCount=" << rightTotalCount << ", "
            << "currentLeftId=" << currentLeftId << ", "
            << "currentRightId=" << currentRightId << ", "
            << "value='" << value << "', "
            << "timestamp=" << timestamp << "}";
        return oss.str();
    }

private:
    std::string key = "";
    bool left = false;
    long leftTotalCount = 0;
    long rightTotalCount = 0;
    long currentLeftId = 0;
    long currentRightId = 0;
    std::string value = "";
    long timestamp = 0;
};

#endif // ORIGINALRECORD_H
