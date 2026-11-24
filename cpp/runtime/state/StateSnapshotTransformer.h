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

#ifndef OMNISTREAM_STATESNAPSHOTTRANSFORMER_H
#define OMNISTREAM_STATESNAPSHOTTRANSFORMER_H

#include <optional>
#include <functional>
#include <memory>

template <typename T>
class StateSnapshotTransformer {
public:
    virtual ~StateSnapshotTransformer() = default;
    virtual std::optional<T> filterOrTransform(const std::optional<T>& value) = 0;
};

template <typename T>
class CollectionStateSnapshotTransformer : public StateSnapshotTransformer<T> {
public:
    enum class TransformStrategy {
        TRANSFORM_ALL,
        STOP_ON_FIRST_INCLUDED
    };

    virtual TransformStrategy getFilterStrategy() const
    {
        return TransformStrategy::TRANSFORM_ALL;
    }
};

template <typename T>
class StateSnapshotTransformFactory {
public:
    virtual ~StateSnapshotTransformFactory() = default;

    virtual std::optional<StateSnapshotTransformer<T>*> createForDeserializedState() = 0;

    virtual std::optional<StateSnapshotTransformer<std::vector<uint8_t>>*> createForSerializedState() = 0;

    template <typename U>
    static std::shared_ptr<StateSnapshotTransformFactory<U>> createNoTransform()
    {
        class NoTransformFactory : public StateSnapshotTransformFactory<U> {
        public:
            std::optional<StateSnapshotTransformer<T>*> createForDeserializedState()
            {
                return std::nullopt;
            }

            std::optional<StateSnapshotTransformer<std::vector<uint8_t>>*> createForSerializedState()
            {
                return std::nullopt;
            }
        };

        return std::make_shared<NoTransformFactory>();
    }
};

#endif // OMNISTREAM_STATESNAPSHOTTRANSFORMER_H
