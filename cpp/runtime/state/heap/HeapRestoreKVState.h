/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#pragma once

#include <tuple>
#include <memory>
#include <vector>

#include <emhash7.hpp>
#include <xxhash.h>

#include "core/memory/DataInputDeserializer.h"
#include "core/typeutils/ListSerializer.h"
#include "core/typeutils/TypeSerializer.h"
#include "core/utils/key_type_traits.h"
#include "runtime/state/HeapKeyedStateBackend.h"
#include "runtime/state/VoidNamespace.h"
#include "runtime/state/heap/CopyOnWriteStateTable.h"
#include "runtime/state/restore/RestoreKVState.h"
#include "table/data/RowData.h"
#include "runtime/state/restore/RestoreBackendDelegate.h"
#include "table/runtime/operators/window/TimeWindow.h"

namespace omnistream {

template <typename K>
class HeapRestoreBackendDelegate;

template <typename K>
class HeapRestoreKVState : public virtual RestoreKVState {
public:
    HeapRestoreKVState(
        HeapRestoreBackendDelegate<K>& delegate, typename HeapRestoreBackendDelegate<K>::RestoreStateInfo& stateInfo)
        : delegate_(delegate),
          stateInfo_(stateInfo),
          keyGroupId_(0)
    {
    }

    void setKeyGroupId(int keyGroupId) override
    {
        keyGroupId_ = keyGroupId;
    }

    void flush() override
    {
        // Heap writes are immediate; nothing to flush.
    }

    void discard() override
    {
        // Heap writes are immediate; nothing to discard.
    }

protected:
    void writeBytesEntry(const std::vector<int8_t>& keyBytes, ByteView value) override;

    void writeValueEntry(const std::vector<int8_t>& keyBytes, ByteView value);

    void writeMapEntry(const std::vector<int8_t>& keyBytes, ByteView value);
    void writeListEntry(const std::vector<int8_t>& keyBytes, ByteView value);
    void ensureMainTableReady();

    // RAII guard：按 namespace 后端类型释放反序列化的 key/namespace 对象。
    // 兼容恢复时 namespace 可能是 VoidNamespace/int64_t/TimeWindow 等多种布局。
    class DeserializedKeyGuard {
    public:
        DeserializedKeyGuard(void* k, void* n, BackendDataType nsType) : rawKey(k), rawNs(n), namespaceType(nsType)
        {
        }
        ~DeserializedKeyGuard()
        {
            release();
        }
        DeserializedKeyGuard(DeserializedKeyGuard&& other) noexcept
            : rawKey(other.rawKey),
              rawNs(other.rawNs),
              namespaceType(other.namespaceType)
        {
            other.rawKey = nullptr;
            other.rawNs = nullptr;
        }

        DeserializedKeyGuard& operator=(DeserializedKeyGuard&& other) noexcept
        {
            if (this != &other) {
                release();
                rawKey = other.rawKey;
                rawNs = other.rawNs;
                namespaceType = other.namespaceType;
                other.rawKey = nullptr;
                other.rawNs = nullptr;
            }
            return *this;
        }

        void* getRawKey() const
        {
            return rawKey;
        }

        void* getRawNamespace() const
        {
            return rawNs;
        }

        DeserializedKeyGuard(const DeserializedKeyGuard&) = delete;
        DeserializedKeyGuard& operator=(const DeserializedKeyGuard&) = delete;

    private:
        void release()
        {
            if (rawKey) {
                if constexpr (std::is_same_v<K, Object*>) {
                    // Release the ref-count acquired by GetBuffer(); the table holds its own reference after put().
                    (*static_cast<Object**>(rawKey))->putRefCount();
                }
                delete static_cast<K*>(rawKey);
            }

            if (rawNs) {
                switch (namespaceType) {
                    case BackendDataType::VOID_NAMESPACE_BK: delete static_cast<VoidNamespace*>(rawNs); break;
                    case BackendDataType::BIGINT_BK: delete static_cast<int64_t*>(rawNs); break;
                    case BackendDataType::TIME_WINDOW_BK: delete static_cast<TimeWindow*>(rawNs); break;
                    default:
                        ERROR_RELEASE("Unknown namespace type: " << namespaceType << ", it will cause a memory leak.");
                }
            }

            rawKey = nullptr;
            rawNs = nullptr;
        }

        void* rawKey;
        void* rawNs;
        BackendDataType namespaceType;
    };

    DeserializedKeyGuard deserializeKey(const std::vector<int8_t>& keyBytes);
    DeserializedKeyGuard deserializeKey(DataInputDeserializer& keyInput);

    HeapRestoreBackendDelegate<K>& delegate_;
    typename HeapRestoreBackendDelegate<K>::RestoreStateInfo& stateInfo_;
    int keyGroupId_;
};

// ============================================================================
// 实现
// ============================================================================

template <typename K>
void HeapRestoreKVState<K>::writeBytesEntry(const std::vector<int8_t>& keyBytes, ByteView value)
{
    if (stateInfo_.stateType == StateDescriptor::Type::MAP) {
        writeMapEntry(keyBytes, value);
    } else if (stateInfo_.stateType == StateDescriptor::Type::LIST) {
        writeListEntry(keyBytes, value);
    } else {
        writeValueEntry(keyBytes, value);
    }
}

template <typename K>
typename HeapRestoreKVState<K>::DeserializedKeyGuard HeapRestoreKVState<K>::deserializeKey(
    const std::vector<int8_t>& keyBytes)
{
    DataInputDeserializer keyInput(
        reinterpret_cast<const uint8_t*>(keyBytes.data()),
        static_cast<int>(keyBytes.size()),
        delegate_.getKeyGroupPrefixBytes());

    return deserializeKey(keyInput);
}

template <typename K>
typename HeapRestoreKVState<K>::DeserializedKeyGuard HeapRestoreKVState<K>::deserializeKey(
    DataInputDeserializer& keyInput)
{
    void* rawKey = nullptr;
    if constexpr (std::is_same_v<K, Object*>) {
        auto* keyObj = delegate_.getKeySerializer()->GetBuffer();
        delegate_.getKeySerializer()->deserialize(keyObj, keyInput);
        rawKey = new Object*(keyObj);
    } else if constexpr (std::is_pointer_v<K>) {
        rawKey = new K(static_cast<K>(delegate_.getKeySerializer()->deserialize(keyInput)));
    } else if constexpr (KeyTypeTraits<K>::isSharedRowKey) {
        using KeyBaseType = unwrap_shared_ptr_t<K>;
        auto* keyBuffer = static_cast<KeyBaseType*>(delegate_.getKeySerializer()->deserialize(keyInput));
        if (keyBuffer == nullptr) {
            INFO_RELEASE(
                "HeapRestoreKVState: Error: deserialized a null shared row key for '" << stateInfo_.stateName << "'");
            throw std::runtime_error("HeapRestoreKVState: deserialized a null shared row key");
        }
        rawKey = new K(std::shared_ptr<KeyBaseType>(static_cast<KeyBaseType*>(keyBuffer->copy())));
    } else if constexpr (is_shared_ptr_v<K>) {
        INFO_RELEASE("HeapRestoreKVState: Error: shared_ptr key not implemented for '" << stateInfo_.stateName << "'");
        throw std::runtime_error("HeapRestoreKVState: shared_ptr key not implemented");
    } else {
        rawKey = delegate_.getKeySerializer()->deserialize(keyInput);
    }

    void* rawNs = stateInfo_.namespaceSerializer->deserialize(keyInput);
    return {rawKey, rawNs, stateInfo_.namespaceSerializer->getBackendId()};
}

template <typename K>
void HeapRestoreKVState<K>::writeValueEntry(const std::vector<int8_t>& keyBytes, ByteView value)
{
    ensureMainTableReady();
    if (stateInfo_.valueSerializer == nullptr) {
        throw std::runtime_error(
            "HeapRestoreKVState: missing value serializer for state '" + stateInfo_.stateName + "'");
    }
    auto keyGuard = deserializeKey(keyBytes);
    auto rawKey = keyGuard.getRawKey();
    auto rawNs = keyGuard.getRawNamespace();

    BackendDataType valueBackendType = stateInfo_.valueSerializer->getBackendId();

    switch (valueBackendType) {
        case BackendDataType::BIGINT_BK:
        case BackendDataType::EXTERNAL_BIGINT_BK: {
            auto* table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, int64_t>*>(stateInfo_.mainTablePtr);
            DataInputDeserializer valInput(value.data(), static_cast<int>(value.size()));
            std::unique_ptr<int64_t> rawVal(static_cast<int64_t*>(stateInfo_.valueSerializer->deserialize(valInput)));
            table->put(*static_cast<K*>(rawKey), keyGroupId_, *static_cast<VoidNamespace*>(rawNs), *rawVal);
            break;
        }
        case BackendDataType::INT_BK: {
            auto* table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, int>*>(stateInfo_.mainTablePtr);
            DataInputDeserializer valInput(value.data(), static_cast<int>(value.size()));
            std::unique_ptr<int> rawVal(static_cast<int*>(stateInfo_.valueSerializer->deserialize(valInput)));
            table->put(*static_cast<K*>(rawKey), keyGroupId_, *static_cast<VoidNamespace*>(rawNs), *rawVal);
            break;
        }
        case BackendDataType::ROW_BK: {
            DataInputDeserializer valInput(value.data(), static_cast<int>(value.size()));
            void* rawVal = stateInfo_.valueSerializer->deserialize(valInput);
            std::unique_ptr<RowData> copied(static_cast<RowData*>(rawVal)->copy());
            // Flink CP 兼容恢复：VALUE 状态的 namespace 可能是 TimeWindow（窗口聚合）、
            // int64_t（interval join）或 VoidNamespace（默认），需按实际表布局分发。
            const auto namespaceBackendType = stateInfo_.namespaceSerializer->getBackendId();
            if (namespaceBackendType == BackendDataType::TIME_WINDOW_BK) {
                auto* table =
                    reinterpret_cast<CopyOnWriteStateTable<K, TimeWindow, RowData*>*>(stateInfo_.mainTablePtr);
                table->put(*static_cast<K*>(rawKey), keyGroupId_, *static_cast<TimeWindow*>(rawNs), copied.release());
            } else if (namespaceBackendType == BackendDataType::BIGINT_BK) {
                auto* table = reinterpret_cast<CopyOnWriteStateTable<K, int64_t, RowData*>*>(stateInfo_.mainTablePtr);
                table->put(*static_cast<K*>(rawKey), keyGroupId_, *static_cast<int64_t*>(rawNs), copied.release());
            } else if (namespaceBackendType == BackendDataType::VOID_NAMESPACE_BK) {
                auto* table =
                    reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, RowData*>*>(stateInfo_.mainTablePtr);
                table->put(
                    *static_cast<K*>(rawKey), keyGroupId_, *static_cast<VoidNamespace*>(rawNs), copied.release());
            } else {
                ERROR_RELEASE(
                    "HeapRestoreKVState: unsupported namespace backend type "
                    << static_cast<int>(namespaceBackendType) << " for ROW VALUE state '" << stateInfo_.stateName
                    << "'");
                throw std::runtime_error(
                    "HeapRestoreKVState: unsupported namespace backend type " +
                    std::to_string(static_cast<int>(namespaceBackendType)) + " for ROW VALUE state '" +
                    stateInfo_.stateName + "'");
            }
            break;
        }
        case BackendDataType::OBJECT_BK:
        case BackendDataType::POJO_BK:
        case BackendDataType::TUPLE_OBJ_OBJ_BK: {
            auto* table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, Object*>*>(stateInfo_.mainTablePtr);
            DataInputDeserializer valInput(value.data(), static_cast<int>(value.size()));
            Object* valObj = stateInfo_.valueSerializer->GetBuffer();
            stateInfo_.valueSerializer->deserialize(valObj, valInput);
            Object* stateVal = valObj->clone();
            valObj->putRefCount();
            table->put(*static_cast<K*>(rawKey), keyGroupId_, *static_cast<VoidNamespace*>(rawNs), stateVal);
            stateVal->putRefCount();
            break;
        }
        case BackendDataType::SET_LONG: {
            auto* table =
                reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, std::vector<long>*>*>(stateInfo_.mainTablePtr);
            DataInputDeserializer valInput(value.data(), static_cast<int>(value.size()));
            auto* vecPtr = static_cast<std::vector<long>*>(stateInfo_.valueSerializer->deserialize(valInput));
            table->put(*static_cast<K*>(rawKey), keyGroupId_, *static_cast<VoidNamespace*>(rawNs), vecPtr);
            break;
        }
        default:
            INFO_RELEASE(
                "HeapRestoreKVState: Error: unsupported value backend type "
                << static_cast<int>(valueBackendType) << " for VALUE state '" << stateInfo_.stateName << "'");
            throw std::runtime_error(
                "HeapRestoreKVState: unsupported value backend type " +
                std::to_string(static_cast<int>(valueBackendType)) + " for VALUE state '" + stateInfo_.stateName + "'");
    }

    stateInfo_.mainEntryCount++;
}

template <typename K>
void HeapRestoreKVState<K>::writeMapEntry(const std::vector<int8_t>& keyBytes, ByteView value)
{
    ensureMainTableReady();
    DataInputDeserializer keyInput(
        reinterpret_cast<const uint8_t*>(keyBytes.data()),
        static_cast<int>(keyBytes.size()),
        delegate_.getKeyGroupPrefixBytes());
    auto keyGuard = deserializeKey(keyInput);
    auto rawKey = keyGuard.getRawKey();
    auto rawNs = keyGuard.getRawNamespace();

    auto* mapKeySer = stateInfo_.mapKeySerializer;
    auto* mapValSer = stateInfo_.mapValueSerializer;
    if (mapKeySer == nullptr || mapValSer == nullptr) {
        INFO_RELEASE(
            "HeapRestoreKVState: Error: MAP state missing key/value serializers for '" << stateInfo_.stateName << "'");
        throw std::runtime_error("HeapRestoreKVState: MAP state missing key/value serializers");
    }

    // Serializer ids describe the checkpoint bytes, while the descriptor ids
    // describe the concrete in-memory table selected by the runtime schema
    // provider (e.g. StreamingJoin maps a ROW checkpoint key onto a
    // SHARED_ROW table). Dispatch must follow the table layout; serializers
    // are only used below to decode the persisted bytes.
    BackendDataType mapKeyId = stateInfo_.mainStateDesc->getKeyDataId();
    BackendDataType mapValId = stateInfo_.mainStateDesc->getValueDataId();

    // SP 统一格式（与 RocksDB per-entry 布局一致）：
    //   key   = [keyGroupPrefix][sourceKey][namespace][mapKey]
    //   value = [null_bool]([mapValue] if !null)
    DataInputDeserializer valInput(value.data(), static_cast<int>(value.size()));
    const bool isMapValNull = valInput.readBoolean();

    if (mapKeyId == BackendDataType::SHARED_ROW_BK && mapValId == BackendDataType::TUPLE_INT32_INT32) {
        using UK = std::shared_ptr<RowData>;
        using UV = std::tuple<int32_t, int32_t>;
        auto* table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, emhash7::HashMap<UK, UV>*>*>(
            stateInfo_.mainTablePtr);

        auto* rawUk = static_cast<RowData*>(mapKeySer->deserialize(keyInput));
        if (rawUk == nullptr) {
            throw std::runtime_error("HeapRestoreKVState: deserialized null shared row map key");
        }
        // RowDataSerializer owns the returned BinaryRowData reuse buffer even
        // when MapSerializer marks its child serializer as non-reusable.
        UK uk(static_cast<RowData*>(rawUk->copy()));
        if (isMapValNull) {
            ERROR_RELEASE(
                "HeapRestoreKVState: unexpected null MAP value for non-pointer type '" << stateInfo_.stateName << "'");
            throw std::runtime_error(
                "HeapRestoreKVState: unexpected null MAP value for non-pointer type '" + stateInfo_.stateName + "'");
        }
        std::unique_ptr<UV> uvPtr(static_cast<UV*>(mapValSer->deserialize(valInput)));
        auto* kvMap = table->get(*static_cast<K*>(rawKey), keyGroupId_, *static_cast<VoidNamespace*>(rawNs));
        if (kvMap == nullptr) {
            kvMap = new emhash7::HashMap<UK, UV>();
            table->put(*static_cast<K*>(rawKey), keyGroupId_, *static_cast<VoidNamespace*>(rawNs), kvMap);
        }
        (*kvMap)[std::move(uk)] = *uvPtr;
    } else if (mapKeyId == BackendDataType::INT_BK && mapValId == BackendDataType::INT_BK) {
        using UK = int;
        using UV = int;
        auto* table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, emhash7::HashMap<UK, UV>*>*>(
            stateInfo_.mainTablePtr);

        std::unique_ptr<UK> ukPtr(static_cast<UK*>(mapKeySer->deserialize(keyInput)));
        UK uk = *ukPtr;
        if (isMapValNull) {
            ERROR_RELEASE(
                "HeapRestoreKVState: unexpected null MAP value for non-pointer type '" << stateInfo_.stateName << "'");
            throw std::runtime_error(
                "HeapRestoreKVState: unexpected null MAP value for non-pointer type '" + stateInfo_.stateName + "'");
        }
        std::unique_ptr<UV> uvPtr(static_cast<UV*>(mapValSer->deserialize(valInput)));
        UV uv = *uvPtr;

        auto* kvMap = table->get(*static_cast<K*>(rawKey), keyGroupId_, *static_cast<VoidNamespace*>(rawNs));
        if (kvMap == nullptr) {
            kvMap = new emhash7::HashMap<UK, UV>();
            (*kvMap)[uk] = uv;
            table->put(*static_cast<K*>(rawKey), keyGroupId_, *static_cast<VoidNamespace*>(rawNs), kvMap);
        } else {
            (*kvMap)[uk] = uv;
        }
    } else if (
        (mapKeyId == BackendDataType::BIGINT_BK && mapValId == BackendDataType::BIGINT_BK) ||
        (mapKeyId == BackendDataType::EXTERNAL_BIGINT_BK && mapValId == BackendDataType::EXTERNAL_BIGINT_BK)) {
        using UK = int64_t;
        using UV = int64_t;
        auto* table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, emhash7::HashMap<UK, UV>*>*>(
            stateInfo_.mainTablePtr);

        std::unique_ptr<UK> ukPtr(static_cast<UK*>(mapKeySer->deserialize(keyInput)));
        UK uk = *ukPtr;
        if (isMapValNull) {
            ERROR_RELEASE(
                "HeapRestoreKVState: unexpected null MAP value for non-pointer type '" << stateInfo_.stateName << "'");
            throw std::runtime_error(
                "HeapRestoreKVState: unexpected null MAP value for non-pointer type '" + stateInfo_.stateName + "'");
        }
        std::unique_ptr<UV> uvPtr(static_cast<UV*>(mapValSer->deserialize(valInput)));
        UV uv = *uvPtr;

        auto* kvMap = table->get(*static_cast<K*>(rawKey), keyGroupId_, *static_cast<VoidNamespace*>(rawNs));
        if (kvMap == nullptr) {
            kvMap = new emhash7::HashMap<UK, UV>();
            (*kvMap)[uk] = uv;
            table->put(*static_cast<K*>(rawKey), keyGroupId_, *static_cast<VoidNamespace*>(rawNs), kvMap);
        } else {
            (*kvMap)[uk] = uv;
        }
    } else if (mapKeyId == BackendDataType::VARCHAR_BK && mapValId == BackendDataType::INT_BK) {
        using UK = std::string;
        using UV = int;
        auto* table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, emhash7::HashMap<UK, UV>*>*>(
            stateInfo_.mainTablePtr);

        auto* rawUk = static_cast<std::string*>(mapKeySer->deserialize(keyInput));
        UK uk = *rawUk;
        delete rawUk;
        if (isMapValNull) {
            ERROR_RELEASE(
                "HeapRestoreKVState: unexpected null MAP value for non-pointer type '" << stateInfo_.stateName << "'");
            throw std::runtime_error(
                "HeapRestoreKVState: unexpected null MAP value for non-pointer type '" + stateInfo_.stateName + "'");
        }
        std::unique_ptr<UV> uvPtr(static_cast<UV*>(mapValSer->deserialize(valInput)));
        UV uv = *uvPtr;

        auto* kvMap = table->get(*static_cast<K*>(rawKey), keyGroupId_, *static_cast<VoidNamespace*>(rawNs));
        if (kvMap == nullptr) {
            kvMap = new emhash7::HashMap<UK, UV>();
            (*kvMap)[std::move(uk)] = uv;
            table->put(*static_cast<K*>(rawKey), keyGroupId_, *static_cast<VoidNamespace*>(rawNs), kvMap);
        } else {
            (*kvMap)[std::move(uk)] = uv;
        }
    } else if (
        (mapKeyId == BackendDataType::OBJECT_BK || mapKeyId == BackendDataType::POJO_BK) &&
        (mapValId == BackendDataType::OBJECT_BK || mapValId == BackendDataType::POJO_BK)) {
        using UK = Object*;
        using UV = Object*;
        auto* table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, emhash7::HashMap<UK, UV>*>*>(
            stateInfo_.mainTablePtr);

        Object* keyObj = mapKeySer->GetBuffer();
        mapKeySer->deserialize(keyObj, keyInput);
        UK uk = keyObj->clone();
        keyObj->putRefCount();

        // 指针类型的 map value 支持 null（写侧 SP 统一格式 [null_bool][mapValue]）
        auto* kvMap = table->get(*static_cast<K*>(rawKey), keyGroupId_, *static_cast<VoidNamespace*>(rawNs));
        if (isMapValNull) {
            if (kvMap == nullptr) {
                kvMap = new emhash7::HashMap<UK, UV>();
                (*kvMap)[uk] = nullptr;
                table->put(*static_cast<K*>(rawKey), keyGroupId_, *static_cast<VoidNamespace*>(rawNs), kvMap);
            } else {
                (*kvMap)[uk] = nullptr;
            }
        } else {
            Object* valObj = mapValSer->GetBuffer();
            mapValSer->deserialize(valObj, valInput);
            UV uv = valObj->clone();
            valObj->putRefCount();

            if (kvMap == nullptr) {
                kvMap = new emhash7::HashMap<UK, UV>();
                (*kvMap)[uk] = uv;
                table->put(*static_cast<K*>(rawKey), keyGroupId_, *static_cast<VoidNamespace*>(rawNs), kvMap);
            } else {
                (*kvMap)[uk] = uv;
            }
        }
    } else if (mapKeyId == BackendDataType::ROW_BK && mapValId == BackendDataType::INT_BK) {
        using UK = RowData*;
        using UV = int32_t;
        auto* table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, emhash7::HashMap<UK, UV>*>*>(
            stateInfo_.mainTablePtr);

        void* rawUk = mapKeySer->deserialize(keyInput);
        UK uk = static_cast<RowData*>(rawUk)->copy();
        if (isMapValNull) {
            ERROR_RELEASE(
                "HeapRestoreKVState: unexpected null MAP value for non-pointer type '" << stateInfo_.stateName << "'");
            throw std::runtime_error(
                "HeapRestoreKVState: unexpected null MAP value for non-pointer type '" + stateInfo_.stateName + "'");
        }
        std::unique_ptr<UV> uvPtr(static_cast<UV*>(mapValSer->deserialize(valInput)));
        UV uv = *uvPtr;

        auto* kvMap = table->get(*static_cast<K*>(rawKey), keyGroupId_, *static_cast<VoidNamespace*>(rawNs));
        if (kvMap == nullptr) {
            kvMap = new emhash7::HashMap<UK, UV>();
            (*kvMap)[uk] = uv;
            table->put(*static_cast<K*>(rawKey), keyGroupId_, *static_cast<VoidNamespace*>(rawNs), kvMap);
        } else {
            (*kvMap)[uk] = uv;
        }
    } else if (mapKeyId == BackendDataType::SHARED_ROW_BK && mapValId == BackendDataType::INT_BK) {
        using UK = std::shared_ptr<RowData>;
        using UV = int32_t;
        auto* table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, emhash7::HashMap<UK, UV>*>*>(
            stateInfo_.mainTablePtr);

        auto* rawUk = static_cast<RowData*>(mapKeySer->deserialize(keyInput));
        if (rawUk == nullptr) {
            throw std::runtime_error("HeapRestoreKVState: deserialized null shared row map key");
        }
        // Keep the serializer-owned buffer alive; only the copied row belongs
        // to the restored shared_ptr key.
        UK uk(static_cast<RowData*>(rawUk->copy()));
        if (isMapValNull) {
            ERROR_RELEASE(
                "HeapRestoreKVState: unexpected null MAP value for non-pointer type '" << stateInfo_.stateName << "'");
            throw std::runtime_error(
                "HeapRestoreKVState: unexpected null MAP value for non-pointer type '" + stateInfo_.stateName + "'");
        }
        std::unique_ptr<UV> uvPtr(static_cast<UV*>(mapValSer->deserialize(valInput)));
        UV uv = *uvPtr;
        auto* kvMap = table->get(*static_cast<K*>(rawKey), keyGroupId_, *static_cast<VoidNamespace*>(rawNs));
        if (kvMap == nullptr) {
            kvMap = new emhash7::HashMap<UK, UV>();
            (*kvMap)[std::move(uk)] = uv;
            table->put(*static_cast<K*>(rawKey), keyGroupId_, *static_cast<VoidNamespace*>(rawNs), kvMap);
        } else {
            (*kvMap)[std::move(uk)] = uv;
        }
    } else if (mapKeyId == BackendDataType::ROW_BK && mapValId == BackendDataType::ROW_BK) {
        using UK = RowData*;
        using UV = RowData*;
        auto* table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, emhash7::HashMap<UK, UV>*>*>(
            stateInfo_.mainTablePtr);

        void* rawUk = mapKeySer->deserialize(keyInput);
        UK uk = static_cast<RowData*>(rawUk)->copy();

        auto* kvMap = table->get(*static_cast<K*>(rawKey), keyGroupId_, *static_cast<VoidNamespace*>(rawNs));
        if (isMapValNull) {
            if (kvMap == nullptr) {
                kvMap = new emhash7::HashMap<UK, UV>();
                (*kvMap)[uk] = nullptr;
                table->put(*static_cast<K*>(rawKey), keyGroupId_, *static_cast<VoidNamespace*>(rawNs), kvMap);
            } else {
                (*kvMap)[uk] = nullptr;
            }
        } else {
            void* rawUv = mapValSer->deserialize(valInput);
            UV uv = static_cast<RowData*>(rawUv)->copy();

            if (kvMap == nullptr) {
                kvMap = new emhash7::HashMap<UK, UV>();
                (*kvMap)[uk] = uv;
                table->put(*static_cast<K*>(rawKey), keyGroupId_, *static_cast<VoidNamespace*>(rawNs), kvMap);
            } else {
                (*kvMap)[uk] = uv;
            }
        }
    } else if (mapKeyId == BackendDataType::TIME_WINDOW_BK && mapValId == BackendDataType::TIME_WINDOW_BK) {
        using UK = TimeWindow;
        using UV = TimeWindow;
        auto* table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, emhash7::HashMap<UK, UV>*>*>(
            stateInfo_.mainTablePtr);

        std::unique_ptr<UK> ukPtr(static_cast<UK*>(mapKeySer->deserialize(keyInput)));
        UK uk = *ukPtr;
        if (isMapValNull) {
            ERROR_RELEASE(
                "HeapRestoreKVState: unexpected null MAP value for non-pointer type '" << stateInfo_.stateName << "'");
            throw std::runtime_error(
                "HeapRestoreKVState: unexpected null MAP value for non-pointer type '" + stateInfo_.stateName + "'");
        }
        std::unique_ptr<UV> uvPtr(static_cast<UV*>(mapValSer->deserialize(valInput)));
        UV uv = *uvPtr;

        auto* kvMap = table->get(*static_cast<K*>(rawKey), keyGroupId_, *static_cast<VoidNamespace*>(rawNs));
        if (kvMap == nullptr) {
            kvMap = new emhash7::HashMap<UK, UV>();
            (*kvMap)[uk] = uv;
            table->put(*static_cast<K*>(rawKey), keyGroupId_, *static_cast<VoidNamespace*>(rawNs), kvMap);
        } else {
            (*kvMap)[uk] = uv;
        }
    } else if (mapKeyId == BackendDataType::ROW_BK && mapValId == BackendDataType::ROW_LIST_BK) {
        using UK = RowData*;
        using UV = std::vector<RowData*>*;
        auto* table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, emhash7::HashMap<UK, UV>*>*>(
            stateInfo_.mainTablePtr);

        void* rawUk = mapKeySer->deserialize(keyInput);
        UK uk = static_cast<RowData*>(rawUk)->copy();

        auto* kvMap = table->get(*static_cast<K*>(rawKey), keyGroupId_, *static_cast<VoidNamespace*>(rawNs));
        if (isMapValNull) {
            if (kvMap == nullptr) {
                kvMap = new emhash7::HashMap<UK, UV>();
                (*kvMap)[uk] = nullptr;
                table->put(*static_cast<K*>(rawKey), keyGroupId_, *static_cast<VoidNamespace*>(rawNs), kvMap);
            } else {
                (*kvMap)[uk] = nullptr;
            }
        } else {
            int listSize = valInput.readInt();
            auto* vec = new std::vector<RowData*>();
            vec->reserve(listSize);
            for (int i = 0; i < listSize; i++) {
                void* rawElem = mapValSer->deserialize(valInput);
                vec->push_back(static_cast<RowData*>(rawElem)->copy());
            }

            if (kvMap == nullptr) {
                kvMap = new emhash7::HashMap<UK, UV>();
                (*kvMap)[uk] = vec;
                table->put(*static_cast<K*>(rawKey), keyGroupId_, *static_cast<VoidNamespace*>(rawNs), kvMap);
            } else {
                (*kvMap)[uk] = vec;
            }
        }
    } else {
        INFO_RELEASE(
            "HeapRestoreKVState: Error: unsupported MAP type combination (keyId="
            << static_cast<int>(mapKeyId) << ", valId=" << static_cast<int>(mapValId) << ") for state '"
            << stateInfo_.stateName << "'");
        throw std::runtime_error(
            "HeapRestoreKVState: unsupported MAP type combination (keyId=" +
            std::to_string(static_cast<int>(mapKeyId)) + ", valId=" + std::to_string(static_cast<int>(mapValId)) +
            ") for state '" + stateInfo_.stateName + "'");
    }

    stateInfo_.mainEntryCount++;
}

template <typename K>
void HeapRestoreKVState<K>::writeListEntry(const std::vector<int8_t>& keyBytes, ByteView value)
{
    ensureMainTableReady();
    auto keyGuard = deserializeKey(keyBytes);
    auto rawKey = keyGuard.getRawKey();
    auto rawNs = keyGuard.getRawNamespace();

    auto* listSer = dynamic_cast<ListSerializer*>(stateInfo_.valueSerializer);
    // If valueSerializer is ListSerializer, extract the element serializer;
    // otherwise, use it directly as the element serializer (e.g. WindowJoin stores LongSerializer).
    TypeSerializer* elemSer = listSer ? listSer->getElementSerializer() : stateInfo_.valueSerializer;
    if (elemSer == nullptr) {
        INFO_RELEASE(
            "HeapRestoreKVState: Error: LIST state has null element serializer for '" << stateInfo_.stateName << "'");
        throw std::runtime_error(
            "HeapRestoreKVState: LIST state has null element serializer for '" + stateInfo_.stateName + "'");
    }

    BackendDataType elemId = elemSer->getBackendId();
    DataInputDeserializer valInput(value.data(), static_cast<int>(value.size()));

    if (elemId == BackendDataType::BIGINT_BK || elemId == BackendDataType::EXTERNAL_BIGINT_BK) {
        // ListDelimitedSerializer format (SP 统一格式，与 RocksDB 一致):
        // [int64_1][','][int64_2][',']...[int64_N] — no size prefix, ',' delimited
        std::unique_ptr<std::vector<int64_t>> vec(new std::vector<int64_t>());
        while (valInput.Available() > 0) {
            std::unique_ptr<int64_t> elemPtr(static_cast<int64_t*>(elemSer->deserialize(valInput)));
            vec->push_back(*elemPtr);
            if (valInput.Available() > 0) {
                valInput.readByte(); // consume ',' delimiter
            }
        }

        auto* table =
            reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, std::vector<int64_t>*>*>(stateInfo_.mainTablePtr);
        table->put(*static_cast<K*>(rawKey), keyGroupId_, *static_cast<VoidNamespace*>(rawNs), vec.release());
    } else {
        ERROR_RELEASE(
            "HeapRestoreKVState: Error: unsupported LIST element type " << static_cast<int>(elemId) << " for state '"
                                                                        << stateInfo_.stateName << "'");
        throw std::runtime_error(
            "HeapRestoreKVState: unsupported LIST element type " + std::to_string(static_cast<int>(elemId)) +
            " for state '" + stateInfo_.stateName + "'");
    }

    stateInfo_.mainEntryCount++;
}

template <typename K>
void HeapRestoreKVState<K>::ensureMainTableReady()
{
    if (stateInfo_.mainStateDesc == nullptr) {
        INFO_RELEASE("HeapRestoreKVState: Error: mainStateDesc is null for '" << stateInfo_.stateName << "'");
        throw std::runtime_error("HeapRestoreKVState: Error: mainStateDesc is null for '" + stateInfo_.stateName + "'");
    }

    if (stateInfo_.mainTablePtr == 0) {
        stateInfo_.mainTablePtr = delegate_.getBackend()->getStateTablePtr(stateInfo_.mainStateDesc->getName());
    }
    if (stateInfo_.mainTablePtr == 0) {
        INFO_RELEASE("HeapRestoreKVState: Error: main table not found for '" << stateInfo_.stateName << "'");
        throw std::runtime_error("HeapRestoreKVState: Error: main table not found for '" + stateInfo_.stateName + "'");
    }
}

} // namespace omnistream
