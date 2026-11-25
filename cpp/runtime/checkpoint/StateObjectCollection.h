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

#ifndef OMNISTREAM_STATEOBJECTCOLLECTION_H
#define OMNISTREAM_STATEOBJECTCOLLECTION_H

#include <vector>
#include <list>
#include <memory>
#include <functional>
#include <algorithm>
#include <iterator>
#include <stdexcept>
#include <string>
#include <sstream>
#include <set>
#include <unordered_set>
#include "runtime/state/StateObject.h"
#include "runtime/state/CompositeStateHandle.h"
#include "core/include/common.h"
#include "runtime/state/InputChannelStateHandle.h"
#include "runtime/state/ResultSubpartitionStateHandle.h"
#include "runtime/snapshot/RocksDBSnapshotStrategyBase.h"

template <typename T>
class StateObjectCollection : public StateObject {
public:
    /** Creates a new StateObjectCollection that is backed by a vector. */
    StateObjectCollection()
    {
        this->stateObjects = std::vector<std::shared_ptr<T>>();
    }

    /**
     * Creates a new StateObjectCollection wraps the given collection and delegates to it.
     *
     * @param stateObjects collection of state objects to wrap.
     */

    explicit StateObjectCollection(std::vector<std::shared_ptr<T>> stateObjects) : stateObjects(stateObjects)
    {
    }

    // Collection interface implementation
    int Size() const
    {
        return static_cast<int>(stateObjects.size());
    }

    bool IsEmpty() const
    {
        return stateObjects.empty();
    }

    bool Contains(const std::shared_ptr<T>& element) const
    {
        return std::find(stateObjects.begin(), stateObjects.end(), element) != stateObjects.end();
    }

    typename std::vector<std::shared_ptr<T>>::iterator begin()
    {
        return stateObjects.begin();
    }

    typename std::vector<std::shared_ptr<T>>::iterator end()
    {
        return stateObjects.end();
    }

    typename std::vector<std::shared_ptr<T>>::const_iterator begin() const
    {
        return stateObjects.begin();
    }

    typename std::vector<std::shared_ptr<T>>::const_iterator end() const
    {
        return stateObjects.end();
    }

    std::vector<std::shared_ptr<T>> ToArray() const
    {
        return stateObjects;
    }

    bool Add(const std::shared_ptr<T>& element)
    {
        stateObjects.push_back(element);
        return true;
    }

    bool Remove(const std::shared_ptr<T>& element)
    {
        auto it = std::find(stateObjects.begin(), stateObjects.end(), element);
        if (it != stateObjects.end()) {
            stateObjects.erase(it);
            return true;
        }
        return false;
    }

    bool ContainsAll(const std::vector<std::shared_ptr<T>>& c) const
    {
        for (const auto& element : c) {
            if (!contains(element)) {
                return false;
            }
        }
        return true;
    }

    bool AddAll(const std::vector<std::shared_ptr<T>>& c)
    {
        bool changed = false;
        for (const auto& element : c) {
            if (add(element)) {
                changed = true;
            }
        }
        return changed;
    }

    bool RemoveAll(const std::vector<std::shared_ptr<T>>& c)
    {
        bool changed = false;
        for (const auto& element : c) {
            if (remove(element)) {
                changed = true;
            }
        }
        return changed;
    }

    bool RemoveIf(std::function<bool(const std::shared_ptr<T>&)> filter)
    {
        auto it = std::remove_if(stateObjects.begin(), stateObjects.end(), filter);
        bool changed = it != stateObjects.end();
        stateObjects.erase(it, stateObjects.end());
        return changed;
    }

    bool RetainAll(const std::vector<std::shared_ptr<T>>& c)
    {
        std::unordered_set<std::shared_ptr<T>> retainSet(c.begin(), c.end());
        auto it =
            std::remove_if(stateObjects.begin(), stateObjects.end(), [&retainSet](const std::shared_ptr<T>& element) {
                return retainSet.find(element) == retainSet.end();
            });
        bool changed = it != stateObjects.end();
        stateObjects.erase(it, stateObjects.end());
        return changed;
    }

    void Clear()
    {
        stateObjects.clear();
    }

    // StateObject interface implementation
    void DiscardState() override
    {
        for (const auto& object : stateObjects) {
            object->DiscardState();
        }
    }

    long GetStateSize() const override
    {
        return SumAllSizes(stateObjects);
    }

    long GetCheckpointedSize() const
    {
        return SumAllCheckpointedSizes(stateObjects);
    }

    /** Returns true if this contains at least one StateObject. */
    bool HasState() const
    {
        for (const auto& state : stateObjects) {
            if (state != nullptr) {
                return true;
            }
        }
        return false;
    }

    bool operator==(const StateObjectCollection<T>& other) const
    {
        if (this == &other) {
            return true;
        }
        // simple equals can cause troubles here because of how equals works e.g. between lists and
        // sets.
        // return CollectionUtils::isEqualCollection(stateObjects, other.stateObjects);
        NOT_IMPL_EXCEPTION;
    }

    bool operator!=(const StateObjectCollection<T>& other) const
    {
        return !(*this == other);
    }

    size_t HashCode() const
    {
        size_t hash = 0;
        for (const auto& obj : stateObjects) {
            hash ^= std::hash<std::shared_ptr<T>>{}(obj);
        }
        return hash;
    }

    std::string ToString() const override
    {
        nlohmann::json j;
        nlohmann::json state_objects_array = nlohmann::json::array();

        for (const auto& obj : stateObjects) {
            if (obj) {
                auto derived = std::dynamic_pointer_cast<BridgeKeyedStateHandle>(obj);
                if (derived != nullptr && derived->handle != nullptr) {
                    std::string jsonStr = derived->handle->ToString();
                    state_objects_array.push_back(nlohmann::json::parse(jsonStr));
                } else {
                    state_objects_array.push_back(nlohmann::json::parse(obj->ToString()));
                }
            } else {
                state_objects_array.push_back(nullptr);
            }
        }
        j["stateObjects"] = state_objects_array;
        return j.dump();
    }

    std::vector<std::shared_ptr<T>> AsList() const
    {
        return stateObjects;
    }

    // ------------------------------------------------------------------------
    //  Helper methods.
    // ------------------------------------------------------------------------

    static std::shared_ptr<StateObjectCollection<T>> Empty()
    {
        if (!EMPTY) {
            EMPTY = std::make_shared<StateObjectCollection<T>>(std::vector<std::shared_ptr<T>>());
        }
        return EMPTY;
    }

    static std::shared_ptr<StateObjectCollection<T>> EmptyIfNull(std::shared_ptr<StateObjectCollection<T>> collection)
    {
        return collection == nullptr ? Empty() : collection;
    }

    static std::shared_ptr<StateObjectCollection<T>> Singleton(std::shared_ptr<T> stateObject)
    {
        std::vector<std::shared_ptr<T>> vec = {stateObject};
        return std::make_shared<StateObjectCollection<T>>(vec);
    }

    static std::shared_ptr<StateObjectCollection<T>> SingletonOrEmpty(std::shared_ptr<T> stateObject)
    {
        return stateObject == nullptr ? Empty() : Singleton(stateObject);
    }

private:
    static const long serialVersionUID = 1L;

    /** The empty StateObjectCollection. */
    static std::shared_ptr<StateObjectCollection<T>> EMPTY;

    /** Wrapped collection that contains the state objects. */
    std::vector<std::shared_ptr<T>> stateObjects;

    // Helper methods
    static long SumAllSizes(const std::vector<std::shared_ptr<T>>& stateObjects)
    {
        long size = 0L;
        for (const auto& object : stateObjects) {
            size += GetSizeNullSafe(object);
        }
        return size;
    }

    static long GetSizeNullSafe(const std::shared_ptr<T>& stateObject)
    {
        return stateObject != nullptr ? stateObject->GetStateSize() : 0L;
    }

    static long SumAllCheckpointedSizes(const std::vector<std::shared_ptr<T>>& stateObjects)
    {
        long size = 0L;
        for (const auto& object : stateObjects) {
            size += GetCheckpointedSizeNullSafe(object);
        }
        return size;
    }

    static long GetCheckpointedSizeNullSafe(const std::shared_ptr<T>& stateObject)
    {
        auto composite = std::dynamic_pointer_cast<CompositeStateHandle>(stateObject);
        return composite != nullptr ? composite->GetCheckpointedSize() : GetSizeNullSafe(stateObject);
    }
};

template <typename T>
inline std::shared_ptr<StateObjectCollection<T>> StateObjectCollection<T>::EMPTY =
    std::make_shared<StateObjectCollection<T>>(std::vector<std::shared_ptr<T>>());
#endif // OMNISTREAM_STATEOBJECTCOLLECTION_H
