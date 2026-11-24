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

#ifndef KEYGROUPSTREAMPARTITIONER_H
#define KEYGROUPSTREAMPARTITIONER_H
#include <nlohmann/json.hpp>
#include <vector>
#include "StreamPartitioner.h"
#include "table/data/binary/BinaryRowData.h"
#include "core/io/IOReadableWritable.h"
#include "table/data/writer/BinaryRowWriter.h"
#include "udf/UDFLoader.h"
#include "functions/KeySelect.h"
#include "basictypes/Tuple2.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "runtime/plugable/SerializationDelegate.h"
#include "table/data/binary/MurmurHashUtils.h"
#include "core/utils/MathUtils.h"
#include "runtime/state/KeyGroupRangeAssignment.h"

using json = nlohmann::json;
/**
 * K: Object
 * */
namespace omnistream::datastream {
template <typename T, typename K>
class KeyGroupStreamPartitioner : public StreamPartitioner<T> {
public:
    KeyGroupStreamPartitioner(nlohmann::json config, int targetId, int maxParallelism, bool recover = false)
        : config(config),
          targetId(targetId),
          maxParallelism(maxParallelism)
    {
        std::string udfObj = config["udf_obj"];
        std::string keySelectorPath = config["hash_path"];
        std::string udfSoPath = config["udf_so"];

        // 算子非对齐input buffer恢复取process算子的input 处理器
        if (recover) {
            nlohmann::json udfObjJson = nlohmann::json::parse(udfObj);
            std::string keySoName = config["key_so"][0];
            std::string keySoPath1 = keySelectorPath + keySoName;
            auto* keySelectorSymbol1 = udfLoader.LoadKeySelectFunction(keySoPath1);
            if (keySelectorSymbol1 == nullptr) {
                INFO_RELEASE("Error:null pointer when load:" << keySoPath1);
                throw std::out_of_range("null pointer when load " + keySoPath1);
            }
            keySelector = keySelectorSymbol1(udfObjJson).release();

        } else {
            std::string keySelectorName = config["hash_so"][std::to_string(targetId)];
            std::string path = keySelectorPath + keySelectorName;
            nlohmann::json udfObjJson = nlohmann::json::parse(udfObj);
            auto symbol = udfLoader.LoadKeySelectFunction(path);
            keySelector = symbol(udfObjJson).release();
        }
        if (maxParallelism <= 0) {
            throw std::invalid_argument("Number of key-groups must be > 0!");
        }
        if (!keySelector) {
            throw std::invalid_argument("Key selector cannot be null");
        }
    }

    ~KeyGroupStreamPartitioner() override
    {
        delete keySelector;
    }

    int getMaxParallelism() const
    {
        return maxParallelism;
    }

    int selectChannel(T* record) override
    {
        K* key;
        try {
            SerializationDelegate* serializationDelegate = reinterpret_cast<SerializationDelegate*>(record);
            StreamRecord* streamRecord = reinterpret_cast<StreamRecord*>(serializationDelegate->getInstance());
            // getkey() function maybe call getPutCount().
            key = keySelector->getKey(static_cast<K*>(streamRecord->getValue()));
        } catch (const std::exception& e) {
            throw std::runtime_error("Could not extract key from ");
        }
        int channel =
            KeyGroupRangeAssignment<K*>::assignKeyToParallelOperator(key, maxParallelism, this->numberOfChannels);
        static_cast<Object*>(key)->putRefCount();
        return channel;
    }

    std::unique_ptr<StreamPartitioner<T>> copy() override
    {
        return std::make_unique<KeyGroupStreamPartitioner<T, K>>(config, targetId, maxParallelism);
    }

    bool isPointWise() const override
    {
        return false;
    }

    [[nodiscard]] std::string toString() const override
    {
        return "HASH";
    }

    void configure(int newMaxParallelism)
    {
        this->maxParallelism = newMaxParallelism;
    }

private:
    UDFLoader udfLoader;
    nlohmann::json config;
    int targetId;
    KeySelect<K>* keySelector = nullptr;
    int maxParallelism;
};
} // namespace omnistream::datastream
#endif // KEYGROUPSTREAMPARTITIONER_H
