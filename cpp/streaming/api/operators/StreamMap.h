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

#ifndef OMNIFLINK_STREAMMAP_H
#define OMNIFLINK_STREAMMAP_H

#include "AbstractUdfStreamOperator.h"
#include "functions/MapFunction.h"
#include "OneInputStreamOperator.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "udf/UDFLoader.h"
#include "connector/kafka/bind_core_manager.h"
namespace omnistream::datastream {
template <typename F, typename K>
class StreamMap : public AbstractUdfStreamOperator<MapFunction<F>, K>, public OneInputStreamOperator {
public:
    explicit StreamMap(Output* output, nlohmann::json config, bool isStream = true)
    {
        LOG("-----create StreamMap-----");
        this->output = output;
        this->isStream = isStream;
        loadUdf(config);
    };

    // only for test
    explicit StreamMap(Output* output, bool isStream = true)
    {
        this->output = output;
        this->isStream = isStream;
    }

    ~StreamMap() override = default;

    void loadUdf(const nlohmann::json& config)
    {
        std::string soPath = config["udf_so"];
        std::string udfObj = config["udf_obj"];
        nlohmann::json udfObjJson = nlohmann::json::parse(udfObj);
        std::cout << "map udf obj: " << udfObj << std::endl;

        auto* symbol = udfLoader.LoadMapFunction(soPath);
        if (symbol == nullptr) {
            throw std::out_of_range("null pointer when load " + soPath);
        }
        function = symbol(udfObjJson);
        this->userFunction = function.release();
    }

    void processElement(StreamRecord* record) override
    {
        if (unlikely(not binded)) {
            if (coreId >= 0) {
                omnistream::BindCoreManager::GetInstance()->BindDirectCore(coreId);
            }
            binded = true;
        }
        LOG("-----StreamMap processElement start -----");
        Object* input = reinterpret_cast<Object*>(record->getValue());
        Object* out = this->userFunction->map(input);
        if (out != nullptr && out == input) {
            out->getRefCount();
        }
        record->setValue(out);
        this->output->collect(record);
        input->putRefCount();
        LOG("-----StreamMap processElement end -----");
    };

    void initializeState(StreamTaskStateInitializerImpl* initializer, TypeSerializer* keySerializer) override
    {
        INFO_RELEASE(
            "StreamMap initializeState with initializer, operatorID: "
            << OneInputStreamOperator::GetOperatorID().toString());
        AbstractStreamOperator<K>::SetOperatorID(OneInputStreamOperator::GetOperatorID().toString());
        AbstractStreamOperator<K>::initializeState(initializer, keySerializer);
        auto taskId = initializer->getEnvironment()->taskConfiguration().getIndexOfSubtask();
        auto& bindCore = omnistream::BindCoreManager::GetInstance();
        if (bindCore->NeedBindOp()) {
            coreId = omnistream::BindCoreManager::GetInstance()->GetOpCore(taskId);
        }
    }
    void initializeState(StateInitializationContextImpl* context) override
    {
        AbstractStreamOperator<K>::initializeState(context);
    }

    void ProcessWatermark(Watermark* watermark) override
    {
        AbstractStreamOperator<K>::ProcessWatermark(watermark);
    }

    void processWatermarkStatus(WatermarkStatus* watermarkStatus) override
    {
        AbstractStreamOperator<K>::processWatermarkStatus(watermarkStatus);
    }

    bool canBeStreamOperator() override
    {
        return this->isStream;
    }

    const char* getName() override
    {
        // NOT IMPLEMENTED
        return "StreamMap";
    }

    void open() override
    {
    }

    void close() override
    {
        AbstractUdfStreamOperator<MapFunction<F>, K>::close();
    }

private:
    UDFLoader udfLoader;
    MapFunctionUnique<F> function;
    int32_t coreId = -1;
    bool binded = false;
};
} // namespace omnistream::datastream
#endif
