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

#include <cstdint>

#include "InternalTimer.h"
#include "table/runtime/operators/window/TimeWindow.h"
#include "core/utils/key_type_traits.h"
#include "runtime/state/VoidNamespace.h"
#include "basictypes/Object.h"
#include "state/heap/HeapPriorityQueueElement.h"

template <typename K, typename N>
class TimerHeapInternalTimer : public InternalTimer<K, N>, public Object, public HeapPriorityQueueElement {
public:
    struct MinHeapComparator {
        bool operator()(
            const std::shared_ptr<TimerHeapInternalTimer>& a, const std::shared_ptr<TimerHeapInternalTimer>& b) const
        {
            // less timestamp has higher priority
            return a->getTimestamp() < b->getTimestamp();
        }
    };

    struct SharedPtrHash {
        size_t operator()(const std::shared_ptr<TimerHeapInternalTimer>& timer) const
        {
            auto timestamp = timer->getTimestamp();
            auto const& key = timer->getKey();
            auto const& nameSpace = timer->getNamespace();
            int32_t result = static_cast<int32_t>(timestamp ^ (timestamp >> 32));
            if constexpr (KeyTypeTraits<K>::isRowKey || KeyTypeTraits<K>::isSharedRowKey) {
                result = 31 * result + key->hashCode();
            } else if constexpr (std::is_same_v<K, Object*>) {
                result = 31 * result + reinterpret_cast<Object*>(key)->hashCode();
            }
            if constexpr (std::is_same_v<N, TimeWindow>) {
                result = 31 * result + nameSpace.hashCode();
            } else if constexpr (std::is_same_v<N, VoidNamespace>) {
                result = 31 * result + ((VoidNamespace)nameSpace).hashCode();
            } else {
                result = 31 * result + nameSpace;
            }
            return result;
        }
    };

    struct SharedPtrEqual {
        bool operator()(
            const std::shared_ptr<TimerHeapInternalTimer>& lhs,
            const std::shared_ptr<TimerHeapInternalTimer>& rhs) const
        {
            if constexpr (KeyTypeTraits<K>::isRowKey || KeyTypeTraits<K>::isSharedRowKey) {
                auto res = lhs->getTimestamp() == rhs->getTimestamp() && lhs->getNamespace() == rhs->getNamespace() &&
                           *lhs->getKey() == *rhs->getKey();
                return res;
            } else if constexpr (std::is_same_v<K, Object*>) {
                auto lkey = reinterpret_cast<Object*>(lhs->getKey());
                auto rkey = reinterpret_cast<Object*>(rhs->getKey());
                if (lkey == nullptr && rkey == nullptr) {
                    return true;
                } else if (lkey == nullptr || rkey == nullptr) {
                    return false;
                }
                auto res = lhs->getTimestamp() == rhs->getTimestamp() && lhs->getNamespace() == rhs->getNamespace() &&
                           lkey->equals(rkey);
                return res;
            } else {
                auto res = lhs->getTimestamp() == rhs->getTimestamp() && lhs->getNamespace() == rhs->getNamespace() &&
                           lhs->getKey() == rhs->getKey();
                return res;
            }
        }
    };

    TimerHeapInternalTimer() = default;
    TimerHeapInternalTimer(int64_t timestamp, K key, N nameSpace) : key(key), nameSpace(nameSpace), timestamp(timestamp)
    {
        if constexpr (std::is_same_v<K, Object*>) {
            reinterpret_cast<Object*>(key)->getRefCount();
        }
        if constexpr (std::is_same_v<N, Object*>) {
            reinterpret_cast<Object*>(nameSpace)->getRefCount();
        }
    };

    ~TimerHeapInternalTimer()
    {
        if constexpr (std::is_same_v<K, Object*>) {
            if (key != nullptr) {
                reinterpret_cast<Object*>(key)->putRefCount();
            }
        }
        if constexpr (std::is_same_v<N, Object*>) {
            if (nameSpace != nullptr) {
                reinterpret_cast<Object*>(nameSpace)->putRefCount();
            }
        }
    }

    K getKey() const override
    {
        return key;
    }

    N getNamespace() const override
    {
        return nameSpace;
    }

    int64_t getTimestamp() const override
    {
        return timestamp;
    }

    bool operator==(TimerHeapInternalTimer& other) const
    {
        if constexpr (KeyTypeTraits<K>::isRowKey || KeyTypeTraits<K>::isSharedRowKey) {
            return this->timestamp == other.timestamp && *this->key == *other.key && this->nameSpace == other.nameSpace;
        } else if constexpr (std::is_same_v<K, Object*>) {
            auto lkey = reinterpret_cast<Object*>(this->key);
            auto rkey = reinterpret_cast<Object*>(other.key);
            if (lkey == nullptr && rkey == nullptr) {
                return true;
            } else if (lkey == nullptr || rkey == nullptr) {
                return false;
            }
            return lkey->equals(rkey) && this->timestamp == other.timestamp && this->nameSpace == other.nameSpace;
        } else {
            return this->timestamp == other.timestamp && this->key == other.key && this->nameSpace == other.nameSpace;
        }
    }

    bool operator!=(TimerHeapInternalTimer<K, N>& other) const
    {
        return !(*this == other);
    }

    inline void setKey(K key_)
    {
        if constexpr (std::is_same_v<K, Object*>) {
            if (key != nullptr) {
                reinterpret_cast<Object*>(key)->putRefCount();
            }
        }
        key = key_;
        if constexpr (std::is_same_v<K, Object*>) {
            reinterpret_cast<Object*>(key)->getRefCount();
        }
    }

    inline void setNamespace(N nameSpace_)
    {
        if constexpr (std::is_same_v<N, Object*>) {
            if (nameSpace != nullptr) {
                reinterpret_cast<Object*>(nameSpace)->putRefCount();
            }
        }
        nameSpace = nameSpace_;
        if constexpr (std::is_same_v<N, Object*>) {
            reinterpret_cast<Object*>(nameSpace)->getRefCount();
        }
    }

    inline void setTimestamp(int64_t timestamp_)
    {
        timestamp = timestamp_;
    }

    void clear()
    {
        if constexpr (std::is_same_v<K, Object*>) {
            reinterpret_cast<Object*>(key)->putRefCount();
        }
        if constexpr (std::is_same_v<N, Object*>) {
            reinterpret_cast<Object*>(nameSpace)->putRefCount();
        }
        return;
    }

private:
    K key{};
    N nameSpace{};
    int64_t timestamp = 0L;
};
