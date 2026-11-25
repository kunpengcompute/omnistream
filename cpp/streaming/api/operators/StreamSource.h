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

#ifndef OMNISTREAM_STREAMSOURCE_H
#define OMNISTREAM_STREAMSOURCE_H

#include <memory>

#include "streaming/api/operators/AbstractStreamOperator.h"
#include "streaming/api/operators/AbstractUdfStreamOperator.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "core/typeinfo/TypeInformation.h"
#include "functions/SourceContext.h"
#include "table/runtime/operators/source/InputFormatSourceFunction.h"
#include "table/runtime/operators/source/InputSplit.h"
#include "jni.h"
#include "basictypes/callback.h"
#include "streaming/api/operators/source/StreamSourceContexts.h"
#include "runtime/tasks/SystemProcessingTimeService.h"
#include "udf/UDFLoader.h"

/**
 * K: such as Object
 * */
namespace omnistream {
template <typename K>
class StreamSource : public AbstractUdfStreamOperator<SourceFunction<K>, K*> {
public:
    StreamSource(SourceFunction<K>* func, Output* output, bool isStream = false)
        : AbstractUdfStreamOperator<SourceFunction<K>, K*>(func, output)
    {
        this->isStream = isStream;
    }

    StreamSource(Output* output, nlohmann::json config, bool isStream = true)
    {
        this->output = output;
        this->isStream = isStream;
        loadUdf(config);
    }

    ~StreamSource()
    {
        delete ctx;
    }

    void loadUdf(const nlohmann::json& config)
    {
        std::string soPath = config["udf_so"];
        std::string udfObj = config["udf_obj"];
        nlohmann::json udfObjJson = nlohmann::json::parse(udfObj);

        auto* symbol = udfLoader.LoadSourceFunction(soPath);
        if (symbol == nullptr) {
            throw std::out_of_range("null pointer when load " + soPath);
        }
        this->userFunction = symbol(udfObjJson).release();
    }

    void setProcessArgs(jmethodID methodID, JNIEnv* env, jobject task)
    {
        CallBack* callback = new CallBack();
        callback->SetArgs(methodID, env, task);
        this->userFunction->SaveCallBack(callback);
    }

    void run()
    {
        thread_local Object lockingObject;
        ctx = StreamSourceContexts::getSourceContext(
            TimeCharacteristic::ProcessingTime,
            new SystemProcessingTimeService(),
            &lockingObject,
            this->output,
            -1,
            -1,
            true,
            this->isStream);
        this->userFunction->run(ctx);
    }

    void run(Object* lock)
    {
        ctx = StreamSourceContexts::getSourceContext(
            TimeCharacteristic::ProcessingTime,
            this->getProcessingTimeService(),
            lock,
            this->output,
            -1,
            -1,
            true,
            this->isStream);
        this->userFunction->run(ctx);
    }

    void stop(StopMode) override
    {
        cancel();
    }

    void cancel()
    {
        this->userFunction->cancel();
    }

    bool canBeStreamOperator() override
    {
        return this->isStream;
    }

private:
    UDFLoader udfLoader;
    SourceContext* ctx;
};
} // namespace omnistream

#endif // OMNISTREAM_STREAMSOURCE_H
