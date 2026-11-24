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

#ifndef OMNISTREAM_KEYEDCOPROCESSOPERATOR_H
#define OMNISTREAM_KEYEDCOPROCESSOPERATOR_H
#include "streaming/api/operators/AbstractUdfStreamOperator.h"
#include "functions/KeyedCoProcessFunction.h"
#include "../TwoInputStreamOperator.h"
#include "api/SimpleTimerService.h"
#include "../TimerHeapInternalTimer.h"
#include "udf/UDFLoader.h"
#include "../TimestampedCollector.h"

namespace omnistream::datastream {
template <typename K, typename IN1, typename IN2, typename OUT>
class KeyedCoProcessOperator : public AbstractUdfStreamOperator<KeyedCoProcessFunction<K*, IN1, IN2, OUT>, OUT>,
                               public TwoInputStreamOperator,
                               public Triggerable<K*, VoidNamespace> {
private:
    class ContextImpl : public KeyedCoProcessFunction<K*, IN1, IN2, OUT>::Context {
    public:
        ContextImpl(
            KeyedCoProcessFunction<K*, IN1, IN2, OUT>* function,
            std::shared_ptr<omnistream::streaming::TimerService> timerService,
            AbstractStreamOperator<K*>* op)
        {
            localTimerService = timerService;
            this->op = op;
            reuse = new StreamRecord();
        }

        ~ContextImpl() override
        {
            delete reuse;
        }

        long timestamp()
        {
            if (element->hasTimestamp()) {
                return element->getTimestamp();
            } else {
                return 0;
            }
        }

        omnistream::streaming::TimerService* timerService() override
        {
            return localTimerService.get();
        }

        void output(Object* value) override
        {
            op->GetOutput()->collect(reuse->replace(value, element->getTimestamp()));
        }

        K* getCurrentKey() override
        {
            return reinterpret_cast<K*>(op->getCurrentKey());
        }

        std::shared_ptr<omnistream::streaming::TimerService> localTimerService;
        AbstractStreamOperator<K*>* op;
        StreamRecord* element;
        StreamRecord* reuse;
    };

    class OnTimerContextImpl : public KeyedCoProcessFunction<K*, IN1, IN2, OUT>::OnTimerContext {
    public:
        std::shared_ptr<omnistream::streaming::TimerService> localTimerService;
        AbstractStreamOperator<K*>* op;
        TimeDomain localTimeDomain;
        StreamRecord* reuse;
        TimerHeapInternalTimer<K*, VoidNamespace>* timer = nullptr;

        OnTimerContextImpl(
            KeyedCoProcessFunction<K*, IN1, IN2, OUT>* function,
            std::shared_ptr<omnistream::streaming::TimerService> timerService,
            AbstractStreamOperator<K*>* op)
        {
            localTimerService = timerService;
            this->op = op;
            reuse = new StreamRecord();
        }

        ~OnTimerContextImpl()
        {
            delete reuse;
        }

        long timestamp() override
        {
            return timer->getTimestamp();
        }

        omnistream::streaming::TimerService* timerService() override
        {
            return localTimerService.get();
        }

        void output(Object* value) override
        {
            op->GetOutput()->collect(reuse->replace(value, timer->getTimestamp()));
        }

        TimeDomain timeDomain() override
        {
            return localTimeDomain;
        }

        K* getCurrentKey() override
        {
            return timer->getKey();
        }
    };

    TimestampedCollector* timestampedCollector = nullptr;
    ContextImpl* context = nullptr;
    OnTimerContextImpl* onTimerContext = nullptr;
    UDFLoader udfLoader;
    KeyedCoProcessFunctionUnique<K*, IN1, IN2, OUT> function = nullptr;
    KeySelectUnique<K> keySelector1;
    KeySelectUnique<K> keySelector2;

public:
    KeyedCoProcessOperator(Output* output, nlohmann::json config, bool isStream = true)
    {
        LOG("-----create KeyedCoProcessOperator-----");
        loadUdf(config);
        this->output = output;
        this->isStream = isStream;
    }

    void processWatermarkStatus1(WatermarkStatus* status) override
    {
        AbstractStreamOperator<OUT>::processWatermarkStatus1(status);
    }

    void processWatermarkStatus2(WatermarkStatus* status) override
    {
        AbstractStreamOperator<OUT>::processWatermarkStatus2(status);
    }

    void loadUdf(const nlohmann::json& config)
    {
        std::string udfSoPath = config["udf_so"];
        std::string udfObj = config["udf_obj"];
        std::string keySoDir = config["hash_path"];
        std::string keySo1Name = config["key_so"][0];
        std::string keySo2Name = config["key_so"][1];

        nlohmann::json udfObjJson = nlohmann::json::parse(udfObj);

        auto* symbol = udfLoader.LoadKeyedCoProcessFunction(udfSoPath);
        if (symbol == nullptr) {
            throw std::out_of_range("null pointer when load " + udfSoPath);
        }
        function = symbol(udfObjJson);
        this->userFunction = function.release();

        std::string keySoPath1 = keySoDir + keySo1Name;
        auto* keySelectorSymbol1 = udfLoader.LoadKeySelectFunction(keySoPath1);
        if (keySelectorSymbol1 == nullptr) {
            throw std::out_of_range("null pointer when load " + keySoPath1);
        }
        keySelector1 = keySelectorSymbol1(udfObjJson);

        std::string keySoPath2 = keySoDir + keySo2Name;
        auto* keySelectorSymbol2 = udfLoader.LoadKeySelectFunction(keySoPath2);
        if (keySelectorSymbol2 == nullptr) {
            throw std::out_of_range("null pointer when load " + keySoPath2);
        }
        keySelector2 = keySelectorSymbol2(udfObjJson);
    }

    ~KeyedCoProcessOperator() override
    {
        delete timestampedCollector;
        delete context;
        delete onTimerContext;
    }

    void open() override
    {
        AbstractUdfStreamOperator<KeyedCoProcessFunction<K*, IN1, IN2, OUT>, K*>::open();
        timestampedCollector = new TimestampedCollector(this->output, this->isStream);

        InternalTimerService<VoidNamespace>* internalTimerService =
            this->template getInternalTimerService<VoidNamespace>("user-timers", new VoidNamespaceSerializer(), this);

        auto timerService = std::make_shared<SimpleTimerService>(internalTimerService);

        context = new ContextImpl(this->userFunction, timerService, this);
        onTimerContext = new OnTimerContextImpl(this->userFunction, timerService, this);
    }

    std::shared_ptr<omnistream::TaskMetricGroup> GetMectrics() override
    {
        return this->metrics;
    }

    void processElement1(StreamRecord* element) override
    {
        LOG("processElement1 handle----------------------");
        timestampedCollector->setTimestamp(element);
        context->element = element;
        auto value = reinterpret_cast<Object*>(element->getValue());
        this->userFunction->processElement1(value, context, timestampedCollector);
        value->putRefCount();
        context->element = nullptr;
    }

    void processElement2(StreamRecord* element) override
    {
        LOG("----------------------processElement2 handle");
        timestampedCollector->setTimestamp(element);
        context->element = element;
        auto value = reinterpret_cast<Object*>(element->getValue());
        this->userFunction->processElement2(value, context, timestampedCollector);
        value->putRefCount();
        context->element = nullptr;
    }

    void ProcessWatermark1(Watermark* watermark) override
    {
        AbstractStreamOperator<OUT>::ProcessWatermark1(watermark);
    }
    void ProcessWatermark2(Watermark* watermark) override
    {
        AbstractStreamOperator<OUT>::ProcessWatermark2(watermark);
    }

    void onEventTime(TimerHeapInternalTimer<K*, VoidNamespace>* timer) override
    {
        timestampedCollector->setAbsoluteTimestamp(timer->getTimestamp());
        onTimerContext->localTimeDomain = TimeDomain::EVENT_TIME;
        onTimerContext->timer = timer;
        this->userFunction->onTimer(timer->getTimestamp(), onTimerContext, timestampedCollector);
        onTimerContext->localTimeDomain = TimeDomain::INVALID;
        onTimerContext->timer = nullptr;
    }

    void onProcessingTime(TimerHeapInternalTimer<K*, VoidNamespace>* timer) override
    {
        timestampedCollector->eraseTimestamp();
        onTimerContext->localTimeDomain = TimeDomain::PROCESSING_TIME;
        onTimerContext->timer = timer;
        this->userFunction->onTimer(timer->getTimestamp(), onTimerContext, timestampedCollector);
        onTimerContext->localTimeDomain = TimeDomain::INVALID;
        onTimerContext->timer = nullptr;
    }

    void initializeState(StreamTaskStateInitializerImpl* initializer, TypeSerializer* keySerializer) override
    {
        // First do the shared initialization step
        INFO_RELEASE(
            "savepoint: KeyedCoProcessOperator initializeState with initializer, operatorID: "
            << TwoInputStreamOperator::GetOperatorID().toString());
        AbstractStreamOperator<K*>::SetOperatorID(TwoInputStreamOperator::GetOperatorID().toString());
        AbstractStreamOperator<K*>::initializeState(initializer, keySerializer);
    }

    void notifyCheckpointComplete(long checkpointId) override
    {
        AbstractUdfStreamOperator<KeyedCoProcessFunction<K*, IN1, IN2, OUT>, OUT>::notifyCheckpointComplete(
            checkpointId);
    }

    void notifyCheckpointAborted(long checkpointId) override
    {
        AbstractUdfStreamOperator<KeyedCoProcessFunction<K*, IN1, IN2, OUT>, OUT>::notifyCheckpointAborted(
            checkpointId);
    }

    bool isSetKeyContextElement1() override
    {
        return true;
    }

    bool isSetKeyContextElement2() override
    {
        return true;
    }

    void setKeyContextElement1(StreamRecord* record) override
    {
        Object* key = reinterpret_cast<Object*>(keySelector1->getKey(reinterpret_cast<Object*>(record->getValue())));
        this->stateHandler->setCurrentKey(key);
        if (key != nullptr) {
            key->putRefCount();
        }
    }

    void setKeyContextElement2(StreamRecord* record) override
    {
        Object* key = reinterpret_cast<Object*>(keySelector2->getKey(reinterpret_cast<Object*>(record->getValue())));
        this->stateHandler->setCurrentKey(key);
        if (key != nullptr) {
            key->putRefCount();
        }
    }

    bool canBeStreamOperator() override
    {
        return this->isStream;
    }

    const char* getName()
    {
        return "KeyedCoProcessOperator";
    }

    void close() override
    {
        AbstractUdfStreamOperator<KeyedCoProcessFunction<K*, IN1, IN2, OUT>, OUT>::close();
    }

protected:
    TimestampedCollector* getCollector()
    {
        return timestampedCollector;
    }
};
} // namespace omnistream::datastream

#endif // OMNISTREAM_KEYEDCOPROCESSOPERATOR_H
