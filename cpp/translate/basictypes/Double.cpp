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

#include "basictypes/Double.h"
#include "core/include/common.h"
Double::Double() = default;
Double::~Double() = default;
Double::Double(double val)
{
    this->value = val;
}
Double* Double::valueOf(double d)
{
    return new Double(d);
};

double Double::jsonValue()
{
    return value;
}

Long* Double::doubleToLongBits(Double* value)
{
    if (value == nullptr) {
        return nullptr;
    }

    if (std::isnan(value->value)) {
        return new Long(0x7ff8000000000000L);
    }
    int64_t result;
    auto ret = memcpy_s(&result, sizeof(int64_t), &value->value, sizeof(double));
    if (unlikely(ret != EOK)) {
        throw std::runtime_error("memcpy_s failed");
    }
    return new Long(result);
}

Double* Double::doubleToLongBits(Long* value)
{
    if (value == nullptr) {
        return nullptr;
    }
    double result;
    auto ret = memcpy_s(&result, sizeof(double), &value->value, sizeof(int64_t));
    if (unlikely(ret != EOK)) {
        throw std::runtime_error("memcpy_s failed");
    }
    return new Double(result);
}

int64_t Double::doubleToLongBits(double value)
{
    if (std::isnan(value)) {
        return 0x7ff8000000000000L;
    }
    int64_t result;
    auto ret = memcpy_s(&result, sizeof(int64_t), &value, sizeof(double));
    if (unlikely(ret != EOK)) {
        throw std::runtime_error("memcpy_s failed");
    }
    return result;
}

double Double::doubleToLongBits(int64_t value)
{
    double result;
    auto ret = memcpy_s(&result, sizeof(double), &value, sizeof(int64_t));
    if (unlikely(ret != EOK)) {
        throw std::runtime_error("memcpy_s failed");
    }
    return result;
}

int Double::hashCode()
{
    int64_t bits = doubleToLongBits(this->value);
    return (int)(bits ^ (bits >> 32));
}

bool Double::equals(Object* obj)
{
    if (obj == nullptr) {
        return false;
    }

    if (typeid(*obj) != typeid(Double)) {
        return false;
    }

    Double* other = static_cast<Double*>(obj);
    if (doubleToLongBits(other->value) != doubleToLongBits(this->value)) {
        return false;
    }

    return true;
}

void Double::setValue(const std::string& basicString)
{
    this->value = std::stod(basicString);
}

Object* Double::clone()
{
    return new Double(value);
}

double Double::doubleValue()
{
    return value;
}
