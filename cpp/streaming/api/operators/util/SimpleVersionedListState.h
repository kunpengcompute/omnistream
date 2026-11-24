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

#include <memory>
#include <string>
#include <vector>
#include <functional>
#include <stdexcept>
#include <iostream>

#include "core/api/common/state/ListState.h"
#include "core/io/SimpleVersionedSerializer.h"
#include "core/io/SimpleVersionedSerialization.h"

// SimpleVersionedListState类
template <typename T>
class SimpleVersionedListState : public ListState<T> {
private:
    // 成员变量
    std::shared_ptr<ListState<std::vector<uint8_t>>> rawState;
    std::shared_ptr<SimpleVersionedSerializer<T>> serializer;

    // 辅助方法
    std::vector<uint8_t> serialize(const T& value) const;
    std::vector<std::vector<uint8_t>>* serializeAll(const std::vector<T>& values) const;
    std::vector<T*>* deserializeAll(const std::vector<std::vector<uint8_t>>* rawValues) const;

public:
    /**
     * 创建一个新的SimpleVersionedListState，使用给定的序列化器从给定的原始ListState中读写字节。
     */
    SimpleVersionedListState(
        std::shared_ptr<ListState<std::vector<uint8_t>>> rawState,
        std::shared_ptr<SimpleVersionedSerializer<T>> serializer);

    virtual ~SimpleVersionedListState() = default;

    // ListState接口方法实现
    void add(const T& value) override;
    void update(const std::vector<T>& values) override;
    std::vector<T>* get() override;
    void merge(const std::vector<T>& other) override;
    void addAll(const std::vector<T>& values) override;
    void clear() override;
    std::vector<T*>* getPtr();
};

// SimpleVersionedListState构造函数实现
template <typename T>
SimpleVersionedListState<T>::SimpleVersionedListState(
    std::shared_ptr<ListState<std::vector<uint8_t>>> rawState, std::shared_ptr<SimpleVersionedSerializer<T>> serializer)
    : rawState(rawState),
      serializer(serializer)
{
    if (!rawState) {
        throw std::invalid_argument("rawState cannot be null");
    }
    if (!serializer) {
        throw std::invalid_argument("serializer cannot be null");
    }
    auto internal = rawState->get();
}

// serialize辅助方法实现
template <typename T>
std::vector<uint8_t> SimpleVersionedListState<T>::serialize(const T& value) const
{
    return SimpleVersionedSerialization::writeVersionAndSerialize(*serializer, value);
}

// serializeAll辅助方法实现
template <typename T>
std::vector<std::vector<uint8_t>>* SimpleVersionedListState<T>::serializeAll(const std::vector<T>& values) const
{
    auto rawValues = new std::vector<std::vector<uint8_t>>();
    rawValues->reserve(values.size());

    for (const auto& value : values) {
        rawValues->push_back(serialize(value));
    }
    return rawValues;
}

// deserializeAll辅助方法实现
template <typename T>
std::vector<T*>* SimpleVersionedListState<T>::deserializeAll(const std::vector<std::vector<uint8_t>>* rawValues) const
{
    if (rawValues == nullptr) {
        return nullptr;
    }

    auto values = new std::vector<T*>();
    values->reserve(rawValues->size());

    for (auto& rawValue : const_cast<std::vector<std::vector<uint8_t>>&>(*rawValues)) {
        values->push_back(SimpleVersionedSerialization::readVersionAndDeSerialize(*serializer, rawValue));
    }

    return values;
}

// add方法实现
template <typename T>
void SimpleVersionedListState<T>::add(const T& value)
{
    auto serialized = serialize(value);
    rawState->add(serialized);
}

// update方法实现
template <typename T>
void SimpleVersionedListState<T>::update(const std::vector<T>& values)
{
    auto rawValues = serializeAll(values);
    rawState->update(*rawValues);
    delete rawValues;
}

// get方法实现
template <typename T>
std::vector<T>* SimpleVersionedListState<T>::get()
{
    auto rawValues = rawState->get();
    auto ptrValues = deserializeAll(rawValues);

    if (ptrValues == nullptr) {
        return nullptr;
    }

    auto values = new std::vector<T>();
    values->reserve(ptrValues->size());

    for (auto ptr : *ptrValues) {
        values->push_back(*ptr);
        delete ptr;
    }

    delete ptrValues;
    return values;
}

// getP方法实现
template <typename T>
std::vector<T*>* SimpleVersionedListState<T>::getPtr()
{
    auto rawValues = rawState->get();
    auto ptrValues = deserializeAll(rawValues);

    if (ptrValues == nullptr) {
        return nullptr;
    }

    auto values = new std::vector<T*>();
    values->reserve(ptrValues->size());

    for (auto ptr : *ptrValues) {
        values->push_back(ptr);
        // delete ptr;
    }

    delete ptrValues;
    return values;
}

// merge方法实现
template <typename T>
void SimpleVersionedListState<T>::merge(const std::vector<T>& other)
{
    auto rawValues = serializeAll(other);
    rawState->merge(*rawValues);
    delete rawValues;
}

// addAll方法实现
template <typename T>
void SimpleVersionedListState<T>::addAll(const std::vector<T>& values)
{
    auto rawValues = serializeAll(values);
    rawState->addAll(*rawValues);
    delete rawValues;
}

// clear方法实现
template <typename T>
void SimpleVersionedListState<T>::clear()
{
    rawState->clear();
}
