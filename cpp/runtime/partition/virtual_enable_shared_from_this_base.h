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

#ifndef OMNISTREAM_virtual_enable_shared_from_this_base_H
#define OMNISTREAM_virtual_enable_shared_from_this_base_H

#include <memory>

namespace omnistream {
struct virtual_enable_shared_from_this_base : std::enable_shared_from_this<virtual_enable_shared_from_this_base> {
public:
    virtual ~virtual_enable_shared_from_this_base()
    {
    }
};
template <typename T>
struct virtual_enable_shared_from_this : virtual virtual_enable_shared_from_this_base {
public:
    std::shared_ptr<T> shared_from_this()
    {
        return std::dynamic_pointer_cast<T>(virtual_enable_shared_from_this_base::shared_from_this());
    }
};
} // namespace omnistream

#endif // OMNISTREAM_virtual_enable_shared_from_this_base_H
