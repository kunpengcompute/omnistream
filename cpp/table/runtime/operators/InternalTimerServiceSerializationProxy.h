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

#ifndef OMNISTREAM_INTERNALTIMERSERVICESERIALIZATIONPROXY_H
#define OMNISTREAM_INTERNALTIMERSERVICESERIALIZATIONPROXY_H

#pragma once

#include <cstdint>

#include <array>

#include "runtime/state/KeyedStateCheckpointOutputStream.h"
#include "runtime/state/restore/RawKeyedStateInputStreamProxy.h"

template <typename K>
class InternalTimeServiceManager;

/**
 * C++ counterpart of Flink's InternalTimerServiceSerializationProxy.
 *
 * The proxy writes one key-group of all registered timer services into the raw
 * keyed state stream. Version is intentionally aligned with Flink 1.16.x.
 */
template <typename K>
class InternalTimerServiceSerializationProxy {
public:
    static constexpr int32_t VERSION = 2;

    // Flink PostVersionedIOReadableWritable.VERSIONED_IDENTIFIER.
    // Java bytes are {-15, -51, -123, -97}, i.e. F1 CD 85 9F.
    inline static constexpr std::array<uint8_t, 4> VERSIONED_IDENTIFIER = {0xF1, 0xCD, 0x85, 0x9F};

    InternalTimerServiceSerializationProxy(InternalTimeServiceManager<K>* timerServicesManager, int32_t keyGroupIdx)
        : timerServicesManager_(timerServicesManager),
          keyGroupIdx_(keyGroupIdx)
    {
    }

    void write(KeyedStateCheckpointOutputStream* out)
    {
        // Keep the wire format identical to Flink 1.16.3:
        // PostVersionedIOReadableWritable.write() first emits VERSIONED_IDENTIFIER,
        // then VersionedIOReadableWritable.write() emits getVersion().
        out->writeBytes(VERSIONED_IDENTIFIER.data(), VERSIONED_IDENTIFIER.size());
        out->writeInt(VERSION);
        timerServicesManager_->writeTimersForKeyGroup(out, keyGroupIdx_);
    }

    void read(RawKeyedStateInputStreamProxy* in)
    {
        for (uint8_t expected : VERSIONED_IDENTIFIER) {
            uint8_t actual = in->readByte();
            if (actual != expected) {
                INFO_RELEASE(
                    "Error: read Invalid Flink timer service serialization header. expected byte="
                    << static_cast<int>(expected) << ", actual byte=" << static_cast<int>(actual));
                THROW_LOGIC_EXCEPTION(
                    "Invalid Flink timer service serialization header. expected byte="
                    << static_cast<int>(expected) << ", actual byte=" << static_cast<int>(actual));
            }
        }

        int32_t version = in->readInt();
        if (version != VERSION) {
            INFO_RELEASE("Error: read Unsupported timer service serialization version: " << version);
            THROW_LOGIC_EXCEPTION("Unsupported timer service serialization version: " << version);
        }
        timerServicesManager_->readTimersForKeyGroup(in, keyGroupIdx_, version);
    }

private:
    InternalTimeServiceManager<K>* timerServicesManager_ = nullptr;
    int32_t keyGroupIdx_ = -1;
};

#endif // OMNISTREAM_INTERNALTIMERSERVICESERIALIZATIONPROXY_H
