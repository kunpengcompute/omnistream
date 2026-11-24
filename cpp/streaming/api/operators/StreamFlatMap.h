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

#ifndef FLINK_TNEL_STREAMFLATMAP1_H
#define FLINK_TNEL_STREAMFLATMAP1_H

#include "AbstractUdfStreamOperator.h"
#include "functions/FlatMapFunction.h"
#include "OneInputStreamOperator.h"
#include "TimestampedCollector.h"
#include "udf/UDFLoader.h"
namespace omnistream::datastream {
template <typename F, typename K>
class StreamFlatMap : public AbstractUdfStreamOperator<FlatMapFunction<F>, K>, public OneInputStreamOperator {
public:
    StreamFlatMap(Output* output, nlohmann::json config, bool isStream = true)
    {
        LOG("-----create StreamFlatMap-----");
        std::string soPath = config["udf_so"];
        std::string udfObj = config["udf_obj"];
        nlohmann::json udfObjJson = nlohmann::json::parse(udfObj);
        std::cout << "flatmap udf obj: " << udfObj << std::endl;
        auto* symbol = udfLoader.LoadFlatMapFunction(soPath);
        if (symbol == nullptr) {
            throw std::out_of_range("null pointer when load " + soPath);
        }
        this->userFunction = symbol(udfObjJson).release();
        this->setOutput(output);
        this->isStream = isStream;
    }

    ~StreamFlatMap() override
    {
        delete collector;
    }

    void open() override
    {
        AbstractUdfStreamOperator<FlatMapFunction<F>, K>::open();
        collector = new TimestampedCollector(this->output, true);
    }

    void processElement(StreamRecord* record) override
    {
        collector->setTimestamp(record);
        Object* input = reinterpret_cast<Object*>(record->getValue());
        this->userFunction->flatMap(input, collector);
        input->putRefCount();
    }

    void initializeState(StreamTaskStateInitializerImpl* initializer, TypeSerializer* keySerializer) override
    {
        // initializeState takes ownership of keySerializer: stateful operators hand it to
        // AbstractKeyedStateBackend, whose destructor frees it. This operator is stateless and
        // has no backend to hand it to, so it frees it here rather than dropping it. The chain
        // allocates a fresh one per operator, so this is never shared. Null for non-SQL types.
        delete keySerializer;
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
        return "StreamFlatMap";
    }

private:
    UDFLoader udfLoader;
    TimestampedCollector* collector;
};
} // namespace omnistream::datastream

#endif
