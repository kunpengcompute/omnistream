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

#ifndef OMNISTREAM_SUPPLIER_H
#define OMNISTREAM_SUPPLIER_H

#include <functional>
#include <memory>
#include <string>

namespace omnistream {

template <typename T>
class Supplier {
public:
    virtual ~Supplier() = default;
    virtual std::shared_ptr<T> get() = 0;
    virtual std::string toString() const = 0;
};

template <typename T>
class LambdaSupplier : public Supplier<T> {
public:
    using SupplierFunction = std::function<std::shared_ptr<T>()>;
    explicit LambdaSupplier(SupplierFunction func) : func_(func)
    {
    }
    ~LambdaSupplier() override = default;

    std::shared_ptr<T> get() override
    {
        return func_();
    }

    std::string toString() const override
    {
        return "LambdaSupplier";
    }

private:
    SupplierFunction func_;
};

} // namespace omnistream

#endif // OMNISTREAM_SUPPLIER_H
