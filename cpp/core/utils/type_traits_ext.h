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
#include <type_traits>
#include <memory>

template <typename T>
struct is_shared_ptr : std::false_type {};
template <typename T>
struct is_shared_ptr<std::shared_ptr<T>> : std::true_type {};
// is_shared_ptr_v<T> return true if T is std::shared_ptr<*>, otherwise return false
template <typename T>
constexpr bool is_shared_ptr_v = is_shared_ptr<typename std::remove_cv_t<typename std::remove_reference_t<T>>>::value;

template <typename T, typename E>
struct is_shared_ptr_of : std::false_type {};
template <typename T>
struct is_shared_ptr_of<std::shared_ptr<T>, T> : std::true_type {};
// is_shared_ptr_of_v<T, E> return true if T is std::shared_ptr<E>, otherwise return false
template <typename T, typename E>
constexpr bool is_shared_ptr_of_v =
    is_shared_ptr_of<typename std::remove_cv_t<typename std::remove_reference_t<T>>, E>::value;

template <typename T>
struct unwrap_shared_ptr {
    using type = std::remove_cv_t<std::remove_reference_t<T>>;
};
template <typename T>
struct unwrap_shared_ptr<std::shared_ptr<T>> {
    using type = T;
};
// unwrap_shared_ptr_t<std::shared_ptr<T>> return T
template <typename T>
using unwrap_shared_ptr_t = typename unwrap_shared_ptr<std::remove_cv_t<std::remove_reference_t<T>>>::type;
