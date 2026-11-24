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

#ifndef OMNISTREAM_AVAILABILITYSTATUS_H
#define OMNISTREAM_AVAILABILITYSTATUS_H
class AvailabilityStatus {
public:
    // 定义内部枚举值，方便做 switch-case 判断或比较
    enum class StatusType {
        AVAILABLE,
        UNAVAILABLE_NEED_REQUESTING_NOTIFICATION,
        UNAVAILABLE_NEED_NOT_REQUESTING_NOTIFICATION
    };

private:
    StatusType type;
    bool available;
    bool needRequestingNotificationOfGlobalPoolAvailable;

    // 私有构造函数，禁止外部随意创建对象
    AvailabilityStatus(StatusType type, bool available, bool needNotify)
        : type(type),
          available(available),
          needRequestingNotificationOfGlobalPoolAvailable(needNotify)
    {
    }

public:
    // 删除拷贝构造和赋值，确保类似Java枚举的单例唯一性
    AvailabilityStatus(const AvailabilityStatus&) = delete;
    AvailabilityStatus& operator=(const AvailabilityStatus&) = delete;

    // --- 静态实例访问器 (模拟 Java Enum 常量) ---
    static const AvailabilityStatus& AVAILABLE()
    {
        static const AvailabilityStatus instance(StatusType::AVAILABLE, true, false);
        return instance;
    }

    static const AvailabilityStatus& UNAVAILABLE_NEED_REQUESTING_NOTIFICATION()
    {
        static const AvailabilityStatus instance(StatusType::UNAVAILABLE_NEED_REQUESTING_NOTIFICATION, false, true);
        return instance;
    }

    static const AvailabilityStatus& UNAVAILABLE_NEED_NOT_REQUESTING_NOTIFICATION()
    {
        static const AvailabilityStatus instance(
            StatusType::UNAVAILABLE_NEED_NOT_REQUESTING_NOTIFICATION, false, false);
        return instance;
    }

    // --- Getter 方法 ---

    bool isAvailable() const
    {
        return available;
    }

    bool isNeedRequestingNotificationOfGlobalPoolAvailable() const
    {
        return needRequestingNotificationOfGlobalPoolAvailable;
    }

    // 获取枚举类型（方便比较）
    StatusType getType() const
    {
        return type;
    }

    // --- 静态工厂方法 (from) ---
    static const AvailabilityStatus& from(bool isAvailable, bool isNeedRequestingNotificationOfGlobalPoolAvailable)
    {
        if (isAvailable) {
            return AVAILABLE();
        } else if (isNeedRequestingNotificationOfGlobalPoolAvailable) {
            return UNAVAILABLE_NEED_REQUESTING_NOTIFICATION();
        } else {
            return UNAVAILABLE_NEED_NOT_REQUESTING_NOTIFICATION();
        }
    }

    // 重载 == 运算符，允许直接比较
    bool operator==(const AvailabilityStatus& other) const
    {
        return this->type == other.type;
    }

    bool operator!=(const AvailabilityStatus& other) const
    {
        return !(*this == other);
    }
};
#endif // OMNISTREAM_AVAILABILITYSTATUS_H
