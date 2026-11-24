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

#ifndef FLINK_TNEL_STATESIZEUTIL_H
#define FLINK_TNEL_STATESIZEUTIL_H

#include <cstdint>
#include <string>
#include <vector>
#include <tuple>
#include <type_traits>
#include <emhash7.hpp>
#include "table/data/RowData.h"
#include "table/data/binary/BinaryRowData.h"
#include "basictypes/Object.h"

// Lightweight, approximate byte-size estimation for keyed-state keys/values.
//
// These helpers size ONE key or ONE value in O(1). Container values
// (std::vector / emhash7::HashMap) are sized as element-count x representative-
// element-width (NOT a full inner walk), since element types are uniform within a
// state. The caller SUMS these over all live entries (one outer walk on the task
// thread) to get the per-state total. Everything here runs on the task thread
// (never the metric-reporter thread), so it never races with put/remove.
//
// The set of (K, N, S) shapes handled is the closed set that
// HeapKeyedStateBackend::createOrUpdateInternalState registers (mirrored by the
// ~HeapKeyedStateBackend() destructor). Any new state type must be added there
// and remain sizable by the rules below.
namespace StateSizeUtil {
// True when the state value S is a pointer to a variable-size container
// (std::vector / emhash7::HashMap). Such values need a per-entry sum walk so each
// container's own element count is counted; fixed-width values (int, RowData*, Object*)
// are uniform and can be sampled once and scaled by entryCount.
template <typename S>
struct is_container_value : std::false_type {};
template <typename E>
struct is_container_value<std::vector<E>*> : std::true_type {};
template <typename UK, typename UV>
struct is_container_value<emhash7::HashMap<UK, UV>*> : std::true_type {};

// Forward declarations so the container overloads can recurse into the
// primary template (and vice-versa) regardless of declaration order.
template <typename T>
inline int64_t sizeInBytes(const T& v);

template <typename E>
inline int64_t sizeInBytes(const std::vector<E>& vec);

template <typename UK, typename UV>
inline int64_t sizeInBytes(const emhash7::HashMap<UK, UV>& map);

inline int64_t sizeInBytes(const std::string& s)
{
    return static_cast<int64_t>(sizeof(s) + s.size());
}

// Primary template: pointers deref-recurse (or use a concrete size method);
// everything else (int, int64_t, std::tuple<...>, VoidNamespace, XXH128_hash_t,
// TimeWindow, ...) is sized by sizeof.
template <typename T>
inline int64_t sizeInBytes(const T& v)
{
    if constexpr (std::is_pointer_v<T>) {
        if (v == nullptr) {
            return 0;
        }
        using Pointee = std::remove_const_t<std::remove_pointer_t<T>>;
        if constexpr (std::is_base_of_v<RowData, Pointee>) {
            // BinaryRowData exposes an O(1) serialized byte length; other
            // RowData subclasses fall back to a shallow size.
            auto* row = dynamic_cast<BinaryRowData*>(v);
            return row ? static_cast<int64_t>(row->getSizeInBytes()) : static_cast<int64_t>(sizeof(Pointee));
        } else if constexpr (std::is_base_of_v<Object, Pointee>) {
            // Object exposes a virtual sizeInBytes(); concrete value-bearing subclasses
            // (String, Tuple2, boxes, ...) override it to include their owned payload.
            return v->sizeInBytes();
        } else if constexpr (std::is_void_v<Pointee> || !std::is_object_v<Pointee>) {
            // void* / function pointers cannot be dereferenced; size them shallowly.
            return static_cast<int64_t>(sizeof(T));
        } else {
            // std::vector<...>*, emhash7::HashMap<...>*, std::tuple<...>* ...
            return sizeInBytes(*v);
        }
    } else {
        return static_cast<int64_t>(sizeof(T));
    }
}

// Container values are sized in O(1) by element COUNT, not a full inner walk:
// element types are uniform within a state, so one representative element's width
// (sampled from the first element) times the element count approximates the total.
template <typename E>
inline int64_t sizeInBytes(const std::vector<E>& vec)
{
    int64_t width = vec.empty() ? 0 : sizeInBytes(vec.front());
    return static_cast<int64_t>(sizeof(vec)) + static_cast<int64_t>(vec.size()) * width;
}

template <typename UK, typename UV>
inline int64_t sizeInBytes(const emhash7::HashMap<UK, UV>& map)
{
    int64_t width = 0;
    if (map.size() != 0) {
        auto it = map.begin();
        width = sizeInBytes(it->first) + sizeInBytes(it->second);
    }
    return static_cast<int64_t>(sizeof(map)) + static_cast<int64_t>(map.size()) * width;
}

// -------- Container helpers for the count-based sum path (CORRECTION 6) --------
// The container sum walk tallies only raw element COUNTS (O(1) per outer key via .size()),
// and the per-element byte WIDTH is sampled exactly ONCE for the whole state (element types
// are uniform within a state). elementCount/sampleElementWidth/containerOverhead are only
// instantiated for container value types (std::vector<E>* / emhash7::HashMap<UK,UV>*).

// Element count of one container value (0 if null). O(1).
template <typename E>
inline int64_t elementCount(std::vector<E>* const& v)
{
    return v ? static_cast<int64_t>(v->size()) : 0;
}
template <typename UK, typename UV>
inline int64_t elementCount(emhash7::HashMap<UK, UV>* const& m)
{
    return m ? static_cast<int64_t>(m->size()) : 0;
}

// Byte width of ONE representative inner element (0 if null/empty). Sampled once per state.
template <typename E>
inline int64_t sampleElementWidth(std::vector<E>* const& v)
{
    return (v && !v->empty()) ? sizeInBytes(v->front()) : 0;
}
template <typename UK, typename UV>
inline int64_t sampleElementWidth(emhash7::HashMap<UK, UV>* const& m)
{
    if (m == nullptr || m->size() == 0) {
        return 0;
    }
    auto it = m->begin();
    return sizeInBytes(it->first) + sizeInBytes(it->second);
}

// Shallow per-container struct overhead (0 if null), counted once per outer key.
template <typename E>
inline int64_t containerOverhead(std::vector<E>* const& v)
{
    return v ? static_cast<int64_t>(sizeof(*v)) : 0;
}
template <typename UK, typename UV>
inline int64_t containerOverhead(emhash7::HashMap<UK, UV>* const& m)
{
    return m ? static_cast<int64_t>(sizeof(*m)) : 0;
}
} // namespace StateSizeUtil

#endif // FLINK_TNEL_STATESIZEUTIL_H
