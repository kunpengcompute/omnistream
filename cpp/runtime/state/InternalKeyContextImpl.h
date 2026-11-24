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

#ifndef FLINK_TNEL_INTERNALKEYCONTEXTIMPL_H
#define FLINK_TNEL_INTERNALKEYCONTEXTIMPL_H

#include "InternalKeyContext.h"
#include "KeyGroupRange.h"
#include "KeyGroupRangeAssignment.h"
#include <string>

template <typename K>
class InternalKeyContextImpl : public InternalKeyContext<K> {
public:
    InternalKeyContextImpl(KeyGroupRange* keyGroupRange, int numberOfKeyGroups)
        : keyGroupRange(keyGroupRange),
          numberOfKeyGroups(numberOfKeyGroups)
    {
        if constexpr (std::is_same_v<K, Object*>) {
            currentKey = nullptr;
        } else if constexpr (std::is_same_v<K, int64_t>) {
            // Initialize currentKey and currentKeyGroupIndex
            currentKey = 0;
        } else if constexpr (std::is_pointer_v<K>) {
            // Initialize currentKey and currentKeyGroupIndex
            currentKey = nullptr;
        }
    };

    ~InternalKeyContextImpl() override
    {
        if constexpr (std::is_same_v<K, Object*>) {
            if (this->currentKey != nullptr) {
                reinterpret_cast<Object*>(this->currentKey)->putRefCount();
            }
        }
    }

    // Getters
    K getCurrentKey() override
    {
        return currentKey;
    };
    int getCurrentKeyGroupIndex() override
    {
        return currentKeyGroupIndex;
    };
    int getNumberOfKeyGroups() override
    {
        return numberOfKeyGroups;
    };
    KeyGroupRange* getKeyGroupRange() override
    {
        return keyGroupRange;
    };

    // Setters
    void setCurrentKey(K currentKey) override
    {
        if constexpr (std::is_same_v<K, Object*>) {
            if (currentKey == nullptr) {
                throw std::runtime_error("Current key is null pointer.");
            }
            reinterpret_cast<Object*>(currentKey)->getRefCount();
        }
        if constexpr (std::is_same_v<K, Object*>) {
            if (this->currentKey != nullptr) {
                reinterpret_cast<Object*>(this->currentKey)->putRefCount();
            }
        }
        this->currentKey = currentKey;
        setCurrentKeyGroupIndex(KeyGroupRangeAssignment<K>::assignToKeyGroup(this->currentKey, getNumberOfKeyGroups()));
    };

    void setCurrentKeyGroupIndex(int currentKeyGroupIndex) override;

private:
    KeyGroupRange* keyGroupRange;
    int numberOfKeyGroups;
    K currentKey;
    int currentKeyGroupIndex;
};

template <typename K>
inline void InternalKeyContextImpl<K>::setCurrentKeyGroupIndex(int currentKeyGroupIndex)
{
    if (!keyGroupRange->contains(currentKeyGroupIndex)) {
        std::string err = "Key group " + std::to_string(currentKeyGroupIndex) + " is not in the range of " +
                          std::to_string(keyGroupRange->getStartKeyGroup()) + " and " +
                          std::to_string(keyGroupRange->getEndKeyGroup());
        THROW_LOGIC_EXCEPTION(err);
    }
    this->currentKeyGroupIndex = currentKeyGroupIndex;
}
#endif // FLINK_TNEL_INTERNALKEYCONTEXTIMPL_H
