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

#ifndef OMNISTREAM_TASKSTATESNAPSHOTDESERIALIZER_H
#define OMNISTREAM_TASKSTATESNAPSHOTDESERIALIZER_H

#include <string>
#include <vector>
#include <map>
#include <optional>
#include <cstdint>

#include "TaskStateSnapshot.h"
#include "runtime/state/IncrementalRemoteKeyedStateHandle.h"
#include "runtime/state/IncrementalLocalKeyedStateHandle.h"
#include "runtime/state/KeyGroupsStateHandle.h"
#include "runtime/state/KeyGroupsSavepointStateHandle.h"
#include "runtime/state/filesystem/RelativeFileStateHandle.h"
#include "runtime/state/DirectoryKeyedStateHandle.h"
#include "runtime/state/OperatorStreamStateHandle.h"
using json = nlohmann::json;

class TaskStateSnapshotDeserializer {
public:
    /**
     * @brief Deserializes a JSON string into a TaskStateSnapshot C++ object.
     *
     * @param json_string The JSON string from Flink's JobManagerTaskRestore.
     * @return A populated TaskStateSnapshot object.
     */
    static std::shared_ptr<TaskStateSnapshot> Deserialize(const std::string& jsonString);

    /**
     * @brief Converts a 32-character hex string into a Flink OperatorID.
     * Reverses the logic of Flink's OperatorID.toHexString().
     * @param hex The hex string (e.g., "ccb29b5204e83e8a588b3828afaa7015").
     * @return A constructed OperatorID object.
     */
    template <typename T>
    static T HexStringToOperatorId(const std::string& hex)
    {
        static_assert(std::is_base_of_v<AbstractIDPOD, T>, "T must inherit from AbstractIDPOD");
        const int maxSize = 32;
        if (hex.length() != maxSize) {
            throw std::runtime_error("Hex string for OperatorID must be 32 characters long.");
        }
        const int steps = 2;
        const auto size = 16;
        std::vector<uint8_t> bytes;
        bytes.reserve(size);
        for (size_t i = 0; i < hex.length(); i += steps) {
            uint8_t byte =
                (static_cast<uint32_t>(HexCharToInt(hex[i])) << 4) | static_cast<uint32_t>(HexCharToInt(hex[i + 1]));
            bytes.push_back(byte);
        }

        if (bytes.size() != size) {
            // This should not happen if input length is 32
            throw std::runtime_error("Failed to convert hex string to 16 bytes.");
        }

        // The Java code writes lowerPart then upperPart.
        // longToByteArray is big-endian.
        int64_t lowerPart = BytesToLongInBigEndian(bytes, 0); // First 8 bytes
        int64_t upperPart = BytesToLongInBigEndian(bytes, 8); // Next 8 bytes

        return T(upperPart, lowerPart);
    }

    /**
     * @brief Helper to convert a single hex character to its integer value.
     */
    static int HexCharToInt(char c)
    {
        const int steps = 10;
        if (c >= '0' && c <= '9') {
            return c - '0';
        }
        if (c >= 'a' && c <= 'f') {
            return c - 'a' + steps;
        }
        if (c >= 'A' && c <= 'F') {
            return c - 'A' + steps;
        }
        throw std::invalid_argument("Invalid hexadecimal character");
    }

    /**
     * @brief Helper to convert a byte array (big-endian) to a 64-bit integer.
     */
    static int64_t BytesToLongInBigEndian(const std::vector<uint8_t>& bytes, size_t offset)
    {
        const int bits = 8;
        uint64_t result = 0;
        for (size_t i = 0; i < bits; ++i) {
            result <<= bits;
            result |= bytes[offset + i];
        }
        return static_cast<int64_t>(result);
    }

private:
    // --- Master "Dispatcher" Parser ---
    // Reads '@class' and calls the correct specific parser below.
    static std::shared_ptr<KeyedStateHandle> ParseKeyedStateHandle(const json& j);

    static std::shared_ptr<OperatorStateHandle> ParseOperatorStateHandle(const json& j);

    static std::shared_ptr<ResultSubpartitionStateHandle> ParseResultStateHandle(const json& j);

    static std::shared_ptr<InputChannelStateHandle> ParseInputStateHandle(const json& j);

    static std::shared_ptr<InflightDataRescalingDescriptor> ParseInflightDataRescalingDescriptor(const json& j);
    // --- Specific Handle Parsers ---
    static std::shared_ptr<IncrementalRemoteKeyedStateHandle> ParseRemoteStateHandle(const json& j)
    {
        auto handle = std::make_shared<IncrementalRemoteKeyedStateHandle>(j);
        return handle;
    }

    static std::shared_ptr<RelativeFileStateHandle> ParseRelativeFileStateHandle(const json& j)
    {
        // New parser for RelativeFileStateHandle
        auto handle = std::make_shared<RelativeFileStateHandle>(j);
        return handle;
    }

    static std::shared_ptr<DirectoryKeyedStateHandle> ParseDirectoryKeyedStateHandle(const json& j)
    {
        // New parser for RelativeFileStateHandle
        auto handle = std::make_shared<DirectoryKeyedStateHandle>(j);
        return handle;
    }

    static std::shared_ptr<DirectoryStateHandle> ParseDirectoryStateHandle(const json& j)
    {
        // New parser for RelativeFileStateHandle
        auto handle = std::make_shared<DirectoryStateHandle>(j);
        return handle;
    }

    static std::shared_ptr<KeyGroupsSavepointStateHandle> ParseKeyGroupsSavepointStateHandle(const json& j)
    {
        return std::make_shared<KeyGroupsSavepointStateHandle>(j);
    }

    static std::shared_ptr<KeyGroupsStateHandle> ParseKeyGroupsStateHandle(const json& j)
    {
        return std::make_shared<KeyGroupsStateHandle>(j);
    }

    static std::shared_ptr<IncrementalLocalKeyedStateHandle> ParseLocalStateHandle(const json& j)
    {
        auto handle = std::make_shared<IncrementalLocalKeyedStateHandle>(j);
        return handle;
    }

    static std::shared_ptr<OperatorSubtaskState> ParseOperatorSubtaskState(const json& j);

    static std::shared_ptr<OperatorStateHandle> parseManagedOperatorStateHandle(const json& j)
    {
        return nullptr;
    }

    template <typename T>
    static std::shared_ptr<StateObjectCollection<T>> ParseStateObjectCollection(
        const json& j, std::shared_ptr<T> (*parser)(const json&));
};

#endif // OMNISTREAM_TASKSTATESNAPSHOTDESERIALIZER_H
