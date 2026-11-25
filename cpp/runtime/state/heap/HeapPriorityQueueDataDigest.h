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

#ifndef OMNISTREAM_HEAPPRIORITYQUEUEDATADIGEST_H
#define OMNISTREAM_HEAPPRIORITYQUEUEDATADIGEST_H

#include <algorithm>
#include <cstdint>
#include <iomanip>
#include <limits>
#include <sstream>
#include <string>
#include <unordered_set>
#include <vector>

#include "common.h"
#include "core/utils/ByteView.h"

class HeapPriorityQueueDataDigest {
public:
    struct Sample {
        bool hasSample = false;
        int keyGroupId = -1;
        uint64_t entryHash = 0;
        uint64_t sampleRankHash = std::numeric_limits<uint64_t>::max();
        uint64_t keyHash = 0;
        uint64_t valueHash = 0;
        size_t keyBytesLen = 0;
        size_t valueBytesLen = 0;
        std::string keyPreviewHex;
        std::string valuePreviewHex;
    };

    struct Summary {
        std::string phase;
        long checkpointId = -1;
        std::string stateName;
        int keyGroupCount = 0;
        uint64_t entryCount = 0;
        uint64_t entryXorHash = 0;
        uint64_t entrySumHash = 0;
        uint64_t keyXorHash = 0;
        uint64_t keySumHash = 0;
        uint64_t valueXorHash = 0;
        uint64_t valueSumHash = 0;
        uint64_t keyGroupXorHash = 0;
        uint64_t keyGroupSumHash = 0;
        size_t keyMinBytes = std::numeric_limits<size_t>::max();
        size_t keyMaxBytes = 0;
        size_t valueMinBytes = std::numeric_limits<size_t>::max();
        size_t valueMaxBytes = 0;
        std::unordered_set<int> nonEmptyKeyGroups;
        Sample sample;
    };

    static Summary createSummary(
        const std::string& phase, long checkpointId, const std::string& stateName, int keyGroupCount)
    {
        Summary summary;
        summary.phase = phase;
        summary.checkpointId = checkpointId;
        summary.stateName = stateName;
        summary.keyGroupCount = keyGroupCount;
        return summary;
    }

    static void addSerializedEntry(
        Summary& summary, const std::vector<int8_t>& key, const std::vector<int8_t>& value, int keyGroupPrefixBytes)
    {
        addSerializedEntry(
            summary,
            ByteView::fromBuffer(key.data(), key.size()),
            ByteView::fromBuffer(value.data(), value.size()),
            keyGroupPrefixBytes);
    }

    static void addSerializedEntry(Summary& summary, ByteView key, ByteView value, int keyGroupPrefixBytes)
    {
        const int keyGroupId = readKeyGroup(key, keyGroupPrefixBytes);
        const uint64_t keyHash = hashBytes(key);
        const uint64_t valueHash = hashBytes(value);
        const uint64_t entryHash = mixHash(keyHash ^ mixHash(valueHash + 0x9e3779b97f4a7c15ULL));

        summary.entryCount++;
        summary.entryXorHash ^= entryHash;
        summary.entrySumHash += entryHash;
        summary.keyXorHash ^= keyHash;
        summary.keySumHash += keyHash;
        summary.valueXorHash ^= valueHash;
        summary.valueSumHash += valueHash;
        summary.keyMinBytes = std::min(summary.keyMinBytes, key.size());
        summary.keyMaxBytes = std::max(summary.keyMaxBytes, key.size());
        summary.valueMinBytes = std::min(summary.valueMinBytes, value.size());
        summary.valueMaxBytes = std::max(summary.valueMaxBytes, value.size());

        if (keyGroupId >= 0) {
            summary.nonEmptyKeyGroups.insert(keyGroupId);
            const uint64_t keyGroupHash = mixHash(static_cast<uint64_t>(keyGroupId) ^ entryHash);
            summary.keyGroupXorHash ^= keyGroupHash;
            summary.keyGroupSumHash += keyGroupHash;
        }

        const uint64_t rankHash = mixHash(entryHash ^ 0xd6e8feb86659fd93ULL);
        if (!summary.sample.hasSample || rankHash < summary.sample.sampleRankHash) {
            summary.sample.hasSample = true;
            summary.sample.keyGroupId = keyGroupId;
            summary.sample.entryHash = entryHash;
            summary.sample.sampleRankHash = rankHash;
            summary.sample.keyHash = keyHash;
            summary.sample.valueHash = valueHash;
            summary.sample.keyBytesLen = key.size();
            summary.sample.valueBytesLen = value.size();
            summary.sample.keyPreviewHex = bytesPreviewHex(key);
            summary.sample.valuePreviewHex = bytesPreviewHex(value);
        }
    }

    static void logSummary(const Summary& summary)
    {
        const size_t keyMinBytes = summary.entryCount == 0 ? 0 : summary.keyMinBytes;
        const size_t valueMinBytes = summary.entryCount == 0 ? 0 : summary.valueMinBytes;

        INFO_RELEASE(
            "TIMER_SP_HEAP_PQ_DATA_DIGEST"
            << " phase=" << summary.phase << ", checkpointId=" << summary.checkpointId << ", backend=ROCKSDB_KEYED"
            << ", pqStorage=HEAP"
            << ", stateName=" << summary.stateName << ", keyGroupCount=" << summary.keyGroupCount
            << ", nonEmptyKeyGroupCount=" << summary.nonEmptyKeyGroups.size() << ", entryCount=" << summary.entryCount
            << ", entryXorHash=" << summary.entryXorHash << ", entrySumHash=" << summary.entrySumHash
            << ", keyXorHash=" << summary.keyXorHash << ", keySumHash=" << summary.keySumHash
            << ", valueXorHash=" << summary.valueXorHash << ", valueSumHash=" << summary.valueSumHash
            << ", keyGroupXorHash=" << summary.keyGroupXorHash << ", keyGroupSumHash=" << summary.keyGroupSumHash
            << ", keyMinBytes=" << keyMinBytes << ", keyMaxBytes=" << summary.keyMaxBytes
            << ", valueMinBytes=" << valueMinBytes << ", valueMaxBytes=" << summary.valueMaxBytes);

        if (!summary.sample.hasSample) {
            return;
        }

        INFO_RELEASE(
            "TIMER_SP_HEAP_PQ_DATA_SAMPLE"
            << " phase=" << summary.phase << ", checkpointId=" << summary.checkpointId << ", backend=ROCKSDB_KEYED"
            << ", pqStorage=HEAP"
            << ", stateName=" << summary.stateName << ", keyGroupId=" << summary.sample.keyGroupId
            << ", entryHash=" << summary.sample.entryHash << ", sampleRankHash=" << summary.sample.sampleRankHash
            << ", keyHash=" << summary.sample.keyHash << ", valueHash=" << summary.sample.valueHash
            << ", keyBytesLen=" << summary.sample.keyBytesLen << ", valueBytesLen=" << summary.sample.valueBytesLen
            << ", keyPreviewHex=" << summary.sample.keyPreviewHex
            << ", valuePreviewHex=" << summary.sample.valuePreviewHex);
    }

private:
    static int readKeyGroup(ByteView key, int keyGroupPrefixBytes)
    {
        if (keyGroupPrefixBytes <= 0 || static_cast<size_t>(keyGroupPrefixBytes) > key.size()) {
            return -1;
        }

        int keyGroup = 0;
        for (int i = 0; i < keyGroupPrefixBytes; i++) {
            keyGroup <<= 8;
            keyGroup |= key[i];
        }
        return keyGroup;
    }

    static uint64_t hashBytes(ByteView bytes)
    {
        if (bytes.empty()) {
            return 0;
        }

        uint64_t hash = 1469598103934665603ULL;
        for (uint8_t byte : bytes) {
            hash ^= byte;
            hash *= 1099511628211ULL;
        }
        return hash;
    }

    static uint64_t mixHash(uint64_t value)
    {
        value += 0x9e3779b97f4a7c15ULL;
        value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9ULL;
        value = (value ^ (value >> 27)) * 0x94d049bb133111ebULL;
        return value ^ (value >> 31);
    }

    static std::string bytesPreviewHex(ByteView bytes)
    {
        constexpr size_t maxPreviewBytes = 24;
        const size_t previewBytes = std::min(bytes.size(), maxPreviewBytes);

        std::ostringstream output;
        output << std::hex << std::setfill('0');
        for (size_t i = 0; i < previewBytes; i++) {
            output << std::setw(2) << static_cast<int>(bytes[i]);
        }
        return output.str();
    }
};

#endif // OMNISTREAM_HEAPPRIORITYQUEUEDATADIGEST_H
