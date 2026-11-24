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

#ifndef MT_CHECK_DOUBLE_H
#define MT_CHECK_DOUBLE_H
#include <libboundscheck/include/securec.h>
#include <typeinfo>
#include "Object.h"
#include "basictypes/Long.h"

class Double : public Object {
public:
    Double(double val);
    Double();
    ~Double();

    static Double* valueOf(double d);
    static Long* doubleToLongBits(Double* value);
    static Double* doubleToLongBits(Long* value);
    static int64_t doubleToLongBits(double value);
    static double doubleToLongBits(int64_t value);
    void setValue(const std::string& basicString) override;
    Object* clone() override;
    int hashCode();
    bool equals(Object* obj);
    double doubleValue();

    double value;
    double jsonValue();

    int64_t sizeInBytes() const override
    {
        return static_cast<int64_t>(sizeof(*this));
    }
};

#endif // MT_CHECK_DOUBLE_H
