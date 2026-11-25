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

#include "core/typeutils/TypeSerializerSingleton.h"
#include "streaming/api/operators/TimerHeapInternalTimer.h"
#include "basictypes/Class.h"
#include "basictypes/Integer.h"
#include "core/utils/key_type_traits.h"

template <typename K, typename N>
class TimerSerializer : public TypeSerializer {
public:
    TimerSerializer(
        TypeSerializer* keySerializer, TypeSerializer* namespaceSerializer, Class* keyClazz, Class* namespaceClazz)
        : keySerializer_(keySerializer),
          namespaceSerializer_(namespaceSerializer),
          keyClazz_(keyClazz),
          namespaceClazz_(namespaceClazz)
    {
    }

    TimerSerializer(TypeSerializer* keySerializer, TypeSerializer* namespaceSerializer)
        : keySerializer_(keySerializer),
          namespaceSerializer_(namespaceSerializer)
    {
        if constexpr (std::is_same_v<Object*, K> && std::is_same_v<N, Object*>) {
            reuseBuffer = static_cast<Object*>(createInstance());
        }
        setSubBufferReusable(false);
    }

    ~TimerSerializer() override
    {
        delete keySerializer_;
        delete namespaceSerializer_;
        delete keyClazz_;
        delete namespaceClazz_;
    }

    void serialize(void* record, DataOutputSerializer& target) override
    {
        NOT_IMPL_EXCEPTION;
    }

    void serialize(Object* buffer, DataOutputSerializer& target) override;

    void deserialize(Object* buffer, DataInputView& source) override;

    void* deserialize(DataInputView& source) override;

    std::shared_ptr<TypeSerializerSnapshot> snapshotConfiguration()
    { // TODO impl build serializer snapshot
        NOT_IMPL_EXCEPTION;
    }

    BackendDataType getBackendId() const override
    {
        return BackendDataType::OBJECT_BK;
    }

    const char* getName() const override
    {
        return "TimerSerializer";
    }

    void setSubBufferReusable(bool bufferReusable_) override;

    Object* GetBuffer() override;

    std::string toJson() override
    {
        SerializerJsonInfo typeJson = {SerializerType::TIMER, "", keySerializer_, nullptr, namespaceSerializer_};
        return typeJson.toJson();
    };

private:
    TimerHeapInternalTimer<Object*, Object*>* createInstance()
    {
        return new TimerHeapInternalTimer<Object*, Object*>(
            0L, keyClazz_->newInstance(), namespaceClazz_->newInstance());
    }

    TypeSerializer* keySerializer_ = nullptr;
    TypeSerializer* namespaceSerializer_ = nullptr;
    Class* keyClazz_ = nullptr;
    Class* namespaceClazz_ = nullptr;
};

template <typename K, typename N>
void TimerSerializer<K, N>::serialize(Object* buffer, DataOutputSerializer& target)
{
    auto timer = static_cast<TimerHeapInternalTimer<K, N>*>(buffer);
    target.writeLong(MathUtils::flipSignBit(timer->getTimestamp()));
    if constexpr (std::is_same_v<K, int64_t> || std::is_same_v<K, int32_t>) {
        auto tempObj = [&timer]() {
            if constexpr (std::is_same_v<K, int64_t>) {
                return new Long(timer->getKey());
            } else if constexpr (std::is_same_v<K, int32_t>) {
                return new Integer(timer->getKey());
            } else {
                NOT_IMPL_EXCEPTION;
            }
        }();
        keySerializer_->serialize(tempObj, target);
        tempObj->putRefCount();
    } else if constexpr (is_shared_ptr_v<K>) {
        keySerializer_->serialize(timer->getKey().get(), target);
    } else {
        keySerializer_->serialize(timer->getKey(), target);
    }

    if constexpr (std::is_same_v<N, Object*>) {
        namespaceSerializer_->serialize(timer->getNamespace(), target);
    } else if constexpr (std::is_same_v<N, int64_t>) {
        auto tempLong = new Long(timer->getNamespace());
        namespaceSerializer_->serialize(tempLong, target);
        delete tempLong;
    } else if constexpr (std::is_same_v<N, VoidNamespace> || std::is_same_v<N, TimeWindow>) {
        auto tempN = timer->getNamespace();
        namespaceSerializer_->serialize(static_cast<N*>(&tempN), target);
    } else {
        NOT_IMPL_EXCEPTION;
    }
}

template <typename K, typename N>
void TimerSerializer<K, N>::deserialize(Object* buffer, DataInputView& source)
{
    auto timer = static_cast<TimerHeapInternalTimer<K, N>*>(buffer);
    long timestamp = MathUtils::flipSignBit(source.readLong());
    timer->setTimestamp(timestamp);

    // deserialize key
    if constexpr (std::is_same_v<K, Object*>) {
        auto keyBuffer = keySerializer_->GetBuffer();
        keySerializer_->deserialize(keyBuffer, source);
        timer->setKey(keyBuffer);
        keyBuffer->putRefCount();
    } else if constexpr (KeyTypeTraits<K>::isRowKey) {
        auto keyBuffer = static_cast<K>(keySerializer_->deserialize(source));
        // todo: the memory is hard to be managed, suggest using std::shared_ptr below
        timer->setKey(keyBuffer->copy());
    } else if constexpr (is_shared_ptr_v<K>) {
        using KeyBaseType = unwrap_shared_ptr_t<K>;
        auto keyBuffer = static_cast<KeyBaseType*>(keySerializer_->deserialize(source));
        timer->setKey(std::shared_ptr<KeyBaseType>(keyBuffer->copy()));
    } else if constexpr (std::is_same_v<K, int64_t> || std::is_same_v<K, int32_t>) {
        auto keyBuffer = [this, &source]() {
            if constexpr (std::is_same_v<K, int64_t>) {
                return static_cast<Long*>(keySerializer_->deserialize(source));
            } else if constexpr (std::is_same_v<K, int32_t>) {
                return static_cast<Integer*>(keySerializer_->deserialize(source));
            } else {
                NOT_IMPL_EXCEPTION;
            }
        }();
        timer->setKey(keyBuffer->getValue());
        keyBuffer->putRefCount();
    } else {
        NOT_IMPL_EXCEPTION;
    }

    // deserialize namespace
    if constexpr (std::is_same_v<N, Object*>) {
        auto namespaceBuffer = namespaceSerializer_->GetBuffer();
        namespaceSerializer_->deserialize(namespaceBuffer, source);
        timer->setNamespace(namespaceBuffer);
        namespaceBuffer->putRefCount();
    } else if constexpr (std::is_same_v<N, int64_t>) {
        auto namespaceBuffer = namespaceSerializer_->GetBuffer();
        namespaceSerializer_->deserialize(namespaceBuffer, source);
        timer->setNamespace(static_cast<Long*>(namespaceBuffer)->getValue());
        namespaceBuffer->putRefCount();
    } else if constexpr (std::is_same_v<N, VoidNamespace>) {
        timer->setNamespace(VoidNamespace());
    } else if constexpr (std::is_same_v<N, TimeWindow>) {
        auto namespaceBuffer =
            static_cast<TimeWindow*>(static_cast<TimeWindow::Serializer*>(namespaceSerializer_)->deserialize(source));
        timer->setNamespace(*namespaceBuffer);
        delete namespaceBuffer;
    } else {
        NOT_IMPL_EXCEPTION;
    }
}

template <typename K, typename N>
void* TimerSerializer<K, N>::deserialize(DataInputView& source)
{
    TimerHeapInternalTimer<K, N>* timer;
    if constexpr (std::is_same_v<K, Object*> && std::is_same_v<N, Object*>) {
        timer = createInstance();
    } else {
        timer = new TimerHeapInternalTimer<K, N>();
    }
    deserialize(timer, source);
    return timer;
}

template <typename K, typename N>
void TimerSerializer<K, N>::setSubBufferReusable(bool bufferReusable_)
{
    keySerializer_->setSelfBufferReusable(bufferReusable_);
    namespaceSerializer_->setSelfBufferReusable(bufferReusable_);
}

template <typename K, typename N>
Object* TimerSerializer<K, N>::GetBuffer()
{
    if (bufferReusable) {
        reuseBuffer->getRefCount();
        return reuseBuffer;
    }
    return static_cast<Object*>(createInstance());
}
