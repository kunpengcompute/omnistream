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

#ifndef FLINK_TNEL_Integer_H
#define FLINK_TNEL_Integer_H

#include "String.h"

class Integer : public Object {
public:
    Integer();

    Integer(int32_t val);

    ~Integer();

    int32_t getValue();

    int32_t jsonValue();

    void setValue(int32_t val);

    int hashCode() override;

    bool equals(Object* obj) override;

    std::string toString() override;

    Object* clone() override;

    int32_t intValue();

    static Integer* valueOf(String* str);

    static Integer* valueOf(int32_t val);

    int32_t value;

    void setValue(const std::string& basicString) override;

    int64_t sizeInBytes() const override
    {
        return static_cast<int64_t>(sizeof(*this));
    }

protected:
    static std::uint32_t parseInt(std::string_view s) noexcept;
};

#endif // FLINK_TNEL_Integer_H
