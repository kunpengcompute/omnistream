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

#ifndef OMNISTREAM_INTERNALTIMERSSNAPSHOTREADERWRITERS_H
#define OMNISTREAM_INTERNALTIMERSSNAPSHOTREADERWRITERS_H

#pragma once

#include <memory>
#include <string>
#include <type_traits>
#include <utility>

#include "basictypes/Integer.h"
#include "basictypes/Long.h"
#include "core/include/common.h"
#include "core/memory/DataOutputSerializer.h"
#include "core/typeutils/LongSerializer.h"
#include "core/typeutils/StringSerializer.h"
#include "core/typeutils/TypeSerializer.h"
#include "core/utils/type_traits_ext.h"
#include "runtime/state/KeyedStateCheckpointOutputStream.h"
#include "runtime/state/VoidNamespace.h"
#include "runtime/state/VoidNamespaceSerializer.h"
#include "runtime/state/restore/RawKeyedStateInputStreamProxy.h"
#include "table/runtime/operators/InternalTimersSnapshot.h"
#include "table/runtime/operators/window/TimeWindow.h"
#include "table/typeutils/BinaryRowDataSerializer.h"
#include "table/typeutils/RowDataSerializer.h"

/**
 * C++ counterpart of Flink's InternalTimersSnapshotReaderWriters.
 *
 * Rocksdb HEAP priority-queue checkpoint timers are raw keyed state.  To allow
 * checkpoints written by Omni to be restored by Flink 1.16.3, the metadata and
 * timer payload below intentionally follow Flink's InternalTimersSnapshotWriterV2:
 *
 *   key serializer TypeSerializerSnapshot
 *   namespace serializer TypeSerializerSnapshot
 *   event-time timer count + timers(key, namespace, timestamp)
 *   processing-time timer count + timers(key, namespace, timestamp)
 */
class FlinkTimerSerializerSnapshots {
public:
    struct SnapshotDescriptor {
        std::string className;
        int32_t version = 0;
        int32_t binaryRowNumFields = -1;
    };

    static constexpr int32_t SIMPLE_TYPE_SERIALIZER_SNAPSHOT_VERSION = 3;

    static constexpr const char* LONG_SERIALIZER_SNAPSHOT =
        "org.apache.flink.api.common.typeutils.base.LongSerializer$LongSerializerSnapshot";
    static constexpr const char* INT_SERIALIZER_SNAPSHOT =
        "org.apache.flink.api.common.typeutils.base.IntSerializer$IntSerializerSnapshot";
    static constexpr const char* STRING_SERIALIZER_SNAPSHOT =
        "org.apache.flink.api.common.typeutils.base.StringSerializer$StringSerializerSnapshot";
    static constexpr const char* VOID_NAMESPACE_SERIALIZER_SNAPSHOT =
        "org.apache.flink.runtime.state.VoidNamespaceSerializer$VoidNamespaceSerializerSnapshot";
    static constexpr const char* TIME_WINDOW_SERIALIZER_SNAPSHOT =
        "org.apache.flink.streaming.api.windowing.windows.TimeWindow$Serializer$TimeWindowSerializerSnapshot";
    static constexpr const char* BINARY_ROW_DATA_SERIALIZER_SNAPSHOT =
        "org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer$BinaryRowDataSerializerSnapshot";

    static void writeVersionedSnapshot(KeyedStateCheckpointOutputStream* out, TypeSerializer* serializer)
    {
        const char* snapshotClassName = snapshotClassNameForSerializer(serializer);
        out->writeUTF(snapshotClassName);
        out->writeInt(SIMPLE_TYPE_SERIALIZER_SNAPSHOT_VERSION);
        if (isBinaryRowDataSerializerSnapshot(snapshotClassName)) {
            out->writeInt(getBinaryRowSerializerArity(serializer));
        }
        // Flink SimpleTypeSerializerSnapshot version 3 has an empty payload.
    }

    static SnapshotDescriptor readVersionedSnapshot(RawKeyedStateInputStreamProxy* in)
    {
        SnapshotDescriptor descriptor;
        descriptor.className = in->readUTF();
        descriptor.version = in->readInt();
        if (descriptor.version != SIMPLE_TYPE_SERIALIZER_SNAPSHOT_VERSION) {
            INFO_RELEASE(
                "Error: readVersionedSnapshot Unsupported Flink timer serializer snapshot version: "
                << descriptor.version << ", className=" << descriptor.className);
            THROW_LOGIC_EXCEPTION(
                "Unsupported Flink timer serializer snapshot version: " << descriptor.version
                                                                        << ", className=" << descriptor.className);
        }
        if (isBinaryRowDataSerializerSnapshot(descriptor.className)) {
            descriptor.binaryRowNumFields = in->readInt();
        }
        // Flink SimpleTypeSerializerSnapshot version 3 has an empty payload.
        return descriptor;
    }

    static TypeSerializer* restoreSerializer(const SnapshotDescriptor& descriptor, TypeSerializer* fallback)
    {
        if (descriptor.className == LONG_SERIALIZER_SNAPSHOT) {
            return LongSerializer::INSTANCE;
        }
        if (descriptor.className == INT_SERIALIZER_SNAPSHOT) {
            return IntSerializer::INSTANCE;
        }
        if (descriptor.className == STRING_SERIALIZER_SNAPSHOT) {
            return StringSerializer::INSTANCE;
        }
        if (descriptor.className == VOID_NAMESPACE_SERIALIZER_SNAPSHOT) {
            return VoidNamespaceSerializer::INSTANCE;
        }
        if (descriptor.className == TIME_WINDOW_SERIALIZER_SNAPSHOT) {
            static TimeWindow::Serializer serializer;
            return &serializer;
        }
        if (descriptor.className == BINARY_ROW_DATA_SERIALIZER_SNAPSHOT) {
            if (fallback != nullptr && fallback->getBackendId() == BackendDataType::ROW_BK) {
                return fallback;
            }
            if (descriptor.binaryRowNumFields >= 0) {
                return new BinaryRowDataSerializer(descriptor.binaryRowNumFields);
            }
        }
        if (fallback != nullptr) {
            return fallback;
        }
        INFO_RELEASE(
            "Error: restoreSerializer Unsupported Flink timer serializer snapshot class: " << descriptor.className);
        THROW_LOGIC_EXCEPTION("Unsupported Flink timer serializer snapshot class: " << descriptor.className);
    }

private:
    static const char* snapshotClassNameForSerializer(TypeSerializer* serializer)
    {
        if (serializer == nullptr) {
            INFO_RELEASE(
                "Error: snapshotClassNameForSerializer Timer serializer is null, cannot write Flink "
                "serializer snapshot.");
            THROW_LOGIC_EXCEPTION("Timer serializer is null, cannot write Flink serializer snapshot.");
        }

        switch (serializer->getBackendId()) {
            case BackendDataType::BIGINT_BK:
            case BackendDataType::LONG_BK: return LONG_SERIALIZER_SNAPSHOT;
            case BackendDataType::INT_BK: return INT_SERIALIZER_SNAPSHOT;
            case BackendDataType::VARCHAR_BK: return STRING_SERIALIZER_SNAPSHOT;
            case BackendDataType::VOID_NAMESPACE_BK: return VOID_NAMESPACE_SERIALIZER_SNAPSHOT;
            case BackendDataType::TIME_WINDOW_BK: return TIME_WINDOW_SERIALIZER_SNAPSHOT;
            case BackendDataType::ROW_BK: return BINARY_ROW_DATA_SERIALIZER_SNAPSHOT;
            default: break;
        }

        std::string name = serializer->getName() == nullptr ? "" : serializer->getName();
        if (name.find("Long") != std::string::npos || name.find("BigInt") != std::string::npos) {
            return LONG_SERIALIZER_SNAPSHOT;
        }
        if (name.find("Int") != std::string::npos) {
            return INT_SERIALIZER_SNAPSHOT;
        }
        if (name.find("String") != std::string::npos) {
            return STRING_SERIALIZER_SNAPSHOT;
        }
        if (name.find("VoidNamespace") != std::string::npos) {
            return VOID_NAMESPACE_SERIALIZER_SNAPSHOT;
        }
        if (name.find("TimeWindow") != std::string::npos) {
            return TIME_WINDOW_SERIALIZER_SNAPSHOT;
        }
        if (name.find("BinaryRowDataSerializer") != std::string::npos ||
            name.find("RowDataSerializer") != std::string::npos) {
            return BINARY_ROW_DATA_SERIALIZER_SNAPSHOT;
        }
        INFO_RELEASE(
            "Error: snapshotClassNameForSerializer Unsupported timer serializer for Flink 1.16.3 CP format. "
            "serializerName="
            << name << ", backendId=" << serializer->getBackendId());
        THROW_LOGIC_EXCEPTION(
            "Unsupported timer serializer for Flink 1.16.3 CP format. serializerName=" << name << ", backendId="
                                                                                       << serializer->getBackendId());
    }

    static bool isBinaryRowDataSerializerSnapshot(const std::string& className)
    {
        return className == BINARY_ROW_DATA_SERIALIZER_SNAPSHOT;
    }

    static bool isBinaryRowDataSerializerSnapshot(const char* className)
    {
        return className != nullptr && std::string(className) == BINARY_ROW_DATA_SERIALIZER_SNAPSHOT;
    }

    static int32_t getBinaryRowSerializerArity(TypeSerializer* serializer)
    {
        if (auto* binaryRowSerializer = dynamic_cast<BinaryRowDataSerializer*>(serializer)) {
            return binaryRowSerializer->getNumFields();
        }
        if (auto* rowSerializer = dynamic_cast<RowDataSerializer*>(serializer)) {
            return rowSerializer->getArity();
        }

        INFO_RELEASE(
            "Error: getBinaryRowSerializerArity Cannot determine BinaryRowDataSerializer arity. serializerName="
            << (serializer == nullptr || serializer->getName() == nullptr ? "" : serializer->getName()));
        THROW_LOGIC_EXCEPTION("Cannot determine BinaryRowDataSerializer arity for Flink timer snapshot.");
    }
};

template <typename K, typename N>
class LegacyTimerSerializer {
public:
    LegacyTimerSerializer(TypeSerializer* keySerializer, TypeSerializer* namespaceSerializer)
        : keySerializer_(keySerializer),
          namespaceSerializer_(namespaceSerializer)
    {
    }

    void serialize(TimerHeapInternalTimer<K, N>* timer, KeyedStateCheckpointOutputStream* out)
    {
        // Flink legacy raw timer format is key, namespace, timestamp. Do not reuse
        // TimerSerializer here because the managed PQ serializer writes timestamp first.
        writeValue<K>(keySerializer_, timer->getKey(), out);
        writeValue<N>(namespaceSerializer_, timer->getNamespace(), out);
        out->writeLong(timer->getTimestamp());
    }

    std::shared_ptr<TimerHeapInternalTimer<K, N>> deserialize(RawKeyedStateInputStreamProxy* in)
    {
        K key = readValue<K>(keySerializer_, in);
        N nameSpace = readValue<N>(namespaceSerializer_, in);
        int64_t timestamp = in->readLong();
        return std::make_shared<TimerHeapInternalTimer<K, N>>(timestamp, key, nameSpace);
    }

private:
    template <typename T>
    void writeValue(TypeSerializer* serializer, T value, KeyedStateCheckpointOutputStream* out)
    {
        if (serializer == nullptr) {
            INFO_RELEASE("Error: writeValue Timer serializer is null.");
            THROW_LOGIC_EXCEPTION("Timer serializer is null.");
        }

        DataOutputSerializer target(128);
        if constexpr (std::is_same_v<T, int64_t>) {
            Long boxed(value);
            serializer->serialize(&boxed, target);
        } else if constexpr (std::is_same_v<T, int32_t>) {
            Integer boxed(value);
            serializer->serialize(&boxed, target);
        } else if constexpr (std::is_same_v<T, Object*>) {
            serializer->serialize(value, target);
        } else if constexpr (is_shared_ptr_v<T>) {
            if (value == nullptr) {
                INFO_RELEASE("Error: writeValue Timer shared_ptr value is null.");
                THROW_LOGIC_EXCEPTION("Timer shared_ptr value is null.");
            }
            serializer->serialize(value.get(), target);
        } else if constexpr (std::is_pointer_v<T>) {
            serializer->serialize(static_cast<void*>(value), target);
        } else {
            T copy = value;
            serializer->serialize(static_cast<void*>(&copy), target);
        }
        out->writeBytes(target.getData(), target.getPosition());
    }

    template <typename T>
    T readValue(TypeSerializer* serializer, RawKeyedStateInputStreamProxy* in)
    {
        if constexpr (std::is_same_v<T, VoidNamespace>) {
            // VoidNamespaceSerializer writes a single marker byte.
            in->readByte();
            return VoidNamespace();
        } else if constexpr (std::is_same_v<T, TimeWindow>) {
            TypeSerializer* effective = serializer == nullptr ? getTimeWindowSerializer() : serializer;
            auto* window = static_cast<TimeWindow*>(effective->deserialize(*in));
            TimeWindow result = *window;
            delete window;
            return result;
        } else if constexpr (std::is_same_v<T, int64_t>) {
            TypeSerializer* effective = serializer == nullptr ? LongSerializer::INSTANCE : serializer;
            auto* value = static_cast<long*>(effective->deserialize(*in));
            int64_t result = static_cast<int64_t>(*value);
            delete value;
            return result;
        } else if constexpr (std::is_same_v<T, int32_t>) {
            TypeSerializer* effective = serializer == nullptr ? IntSerializer::INSTANCE : serializer;
            auto* value = static_cast<int*>(effective->deserialize(*in));
            int32_t result = static_cast<int32_t>(*value);
            delete value;
            return result;
        } else if constexpr (std::is_same_v<T, Object*>) {
            if (serializer == nullptr) {
                INFO_RELEASE("Error: readValue Cannot deserialize Object* timer key without serializer.");
                THROW_LOGIC_EXCEPTION("Cannot deserialize Object* timer key without serializer.");
            }
            Object* buffer = serializer->GetBuffer();
            if (buffer == nullptr) {
                INFO_RELEASE(
                    "Error: readValue Cannot deserialize Object* timer key because serializer returned null "
                    "buffer.");
                THROW_LOGIC_EXCEPTION("Cannot deserialize Object* timer key because serializer returned null buffer.");
            }
            serializer->deserialize(buffer, *in);
            return buffer;
        } else if constexpr (std::is_pointer_v<T>) {
            if (serializer == nullptr) {
                INFO_RELEASE("Error: readValue Cannot deserialize pointer timer value without serializer.");
                THROW_LOGIC_EXCEPTION("Cannot deserialize pointer timer value without serializer.");
            }
            auto* value = static_cast<T>(serializer->deserialize(*in));
            if constexpr (std::is_base_of_v<RowData, std::remove_pointer_t<T>>) {
                return value == nullptr ? nullptr : static_cast<T>(value->copy());
            } else {
                return value;
            }
        } else if constexpr (is_shared_ptr_v<T>) {
            if (serializer == nullptr) {
                INFO_RELEASE("Error: readValue Cannot deserialize shared_ptr timer value without serializer.");
                THROW_LOGIC_EXCEPTION("Cannot deserialize shared_ptr timer value without serializer.");
            }
            using ElementType = unwrap_shared_ptr_t<T>;
            auto* value = static_cast<ElementType*>(serializer->deserialize(*in));
            if (value == nullptr) {
                return T();
            }
            if constexpr (std::is_base_of_v<RowData, ElementType>) {
                return T(static_cast<ElementType*>(value->copy()));
            } else {
                return T(new ElementType(*value));
            }
        } else {
            if (serializer == nullptr) {
                INFO_RELEASE("Error: readValue Cannot deserialize timer value without serializer.");
                THROW_LOGIC_EXCEPTION("Cannot deserialize timer value without serializer.");
            }
            auto* value = static_cast<T*>(serializer->deserialize(*in));
            T result = *value;
            delete value;
            return result;
        }
    }

    static TypeSerializer* getTimeWindowSerializer()
    {
        static TimeWindow::Serializer serializer;
        return &serializer;
    }

    TypeSerializer* keySerializer_ = nullptr;
    TypeSerializer* namespaceSerializer_ = nullptr;
};

template <typename K, typename N>
class InternalTimersSnapshotWriter {
public:
    virtual ~InternalTimersSnapshotWriter() = default;
    virtual void writeTimersSnapshot(KeyedStateCheckpointOutputStream* out) = 0;
};

template <typename K, typename N>
class InternalTimersSnapshotReader {
public:
    virtual ~InternalTimersSnapshotReader() = default;
    virtual InternalTimersSnapshot<K, N> readTimersSnapshot(RawKeyedStateInputStreamProxy* in) = 0;
};

template <typename K, typename N>
class InternalTimersSnapshotWriterV2 : public InternalTimersSnapshotWriter<K, N> {
public:
    InternalTimersSnapshotWriterV2(
        InternalTimersSnapshot<K, N> snapshot, TypeSerializer* keySerializer, TypeSerializer* namespaceSerializer)
        : snapshot_(std::move(snapshot)),
          keySerializer_(keySerializer),
          namespaceSerializer_(namespaceSerializer)
    {
    }

    void writeTimersSnapshot(KeyedStateCheckpointOutputStream* out) override
    {
        FlinkTimerSerializerSnapshots::writeVersionedSnapshot(out, keySerializer_);
        FlinkTimerSerializerSnapshots::writeVersionedSnapshot(out, namespaceSerializer_);

        LegacyTimerSerializer<K, N> timerSerializer(keySerializer_, namespaceSerializer_);

        const auto& eventTimers = snapshot_.getEventTimeTimers();
        out->writeInt(static_cast<int32_t>(eventTimers.size()));
        for (const auto& timer : eventTimers) {
            timerSerializer.serialize(timer.get(), out);
        }

        const auto& processingTimers = snapshot_.getProcessingTimeTimers();
        out->writeInt(static_cast<int32_t>(processingTimers.size()));
        for (const auto& timer : processingTimers) {
            timerSerializer.serialize(timer.get(), out);
        }
    }

private:
    InternalTimersSnapshot<K, N> snapshot_;
    TypeSerializer* keySerializer_ = nullptr;
    TypeSerializer* namespaceSerializer_ = nullptr;
};

template <typename K, typename N>
class InternalTimersSnapshotReaderV2 : public InternalTimersSnapshotReader<K, N> {
public:
    InternalTimersSnapshotReaderV2(
        FlinkTimerSerializerSnapshots::SnapshotDescriptor keySerializerSnapshot,
        FlinkTimerSerializerSnapshots::SnapshotDescriptor namespaceSerializerSnapshot,
        TypeSerializer* fallbackKeySerializer)
        : keySerializerSnapshot_(std::move(keySerializerSnapshot)),
          namespaceSerializerSnapshot_(std::move(namespaceSerializerSnapshot)),
          fallbackKeySerializer_(fallbackKeySerializer)
    {
    }

    InternalTimersSnapshot<K, N> readTimersSnapshot(RawKeyedStateInputStreamProxy* in) override
    {
        TypeSerializer* keySerializer =
            FlinkTimerSerializerSnapshots::restoreSerializer(keySerializerSnapshot_, fallbackKeySerializer_);
        TypeSerializer* namespaceSerializer = FlinkTimerSerializerSnapshots::restoreSerializer(
            namespaceSerializerSnapshot_, defaultNamespaceSerializer());

        InternalTimersSnapshot<K, N> snapshot;
        snapshot.setKeySerializer(keySerializer);
        snapshot.setNamespaceSerializer(namespaceSerializer);

        LegacyTimerSerializer<K, N> timerSerializer(keySerializer, namespaceSerializer);

        int32_t eventTimerCount = in->readInt();
        for (int32_t i = 0; i < eventTimerCount; ++i) {
            snapshot.addEventTimeTimer(timerSerializer.deserialize(in));
        }

        int32_t processingTimerCount = in->readInt();
        for (int32_t i = 0; i < processingTimerCount; ++i) {
            snapshot.addProcessingTimeTimer(timerSerializer.deserialize(in));
        }

        return snapshot;
    }

private:
    TypeSerializer* defaultNamespaceSerializer()
    {
        if constexpr (std::is_same_v<N, VoidNamespace>) {
            return VoidNamespaceSerializer::INSTANCE;
        } else if constexpr (std::is_same_v<N, TimeWindow>) {
            static TimeWindow::Serializer serializer;
            return &serializer;
        } else if constexpr (std::is_same_v<N, int64_t>) {
            return LongSerializer::INSTANCE;
        } else {
            return nullptr;
        }
    }

    FlinkTimerSerializerSnapshots::SnapshotDescriptor keySerializerSnapshot_;
    FlinkTimerSerializerSnapshots::SnapshotDescriptor namespaceSerializerSnapshot_;
    TypeSerializer* fallbackKeySerializer_ = nullptr;
};

template <typename K, typename N>
class InternalTimersSnapshotReaderWriters {
public:
    static constexpr int32_t VERSION = 2;

    static std::unique_ptr<InternalTimersSnapshotWriter<K, N>> getWriterForVersion(
        int32_t version,
        InternalTimersSnapshot<K, N> snapshot,
        TypeSerializer* keySerializer,
        TypeSerializer* namespaceSerializer)
    {
        if (version != VERSION) {
            INFO_RELEASE("Error: getWriterForVersion Unsupported timer snapshot writer version: " << version);
            THROW_LOGIC_EXCEPTION("Unsupported timer snapshot writer version: " << version);
        }
        return std::make_unique<InternalTimersSnapshotWriterV2<K, N>>(
            std::move(snapshot), keySerializer, namespaceSerializer);
    }

    static std::unique_ptr<InternalTimersSnapshotReader<K, N>> getReaderForVersion(
        int32_t version,
        FlinkTimerSerializerSnapshots::SnapshotDescriptor keySerializerSnapshot,
        FlinkTimerSerializerSnapshots::SnapshotDescriptor namespaceSerializerSnapshot,
        TypeSerializer* fallbackKeySerializer)
    {
        if (version != VERSION) {
            INFO_RELEASE("Error: getReaderForVersion Unsupported timer snapshot reader version: " << version);
            THROW_LOGIC_EXCEPTION("Unsupported timer snapshot reader version: " << version);
        }
        return std::make_unique<InternalTimersSnapshotReaderV2<K, N>>(
            std::move(keySerializerSnapshot), std::move(namespaceSerializerSnapshot), fallbackKeySerializer);
    }
};

#endif // OMNISTREAM_INTERNALTIMERSSNAPSHOTREADERWRITERS_H
