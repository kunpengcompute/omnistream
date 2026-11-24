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

#ifndef FLINK_TNEL_PROCESSOPERATOR_H
#define FLINK_TNEL_PROCESSOPERATOR_H

#include "streaming/api/operators/OneInputStreamOperator.h"
#include "streaming/api/operators/AbstractUdfStreamOperator.h"
#include "streaming/api/functions/ProcessFunction.h"
#include "TimestampedCollector.h"
#include "core/udf/UDFLoader.h"

// This class is not keyed, but AbstractUdfStreamOperator's base AbtractStreamOperator is, so I will give it a fake
// K=int LookupJoin's ProcessFunction<IN, OUT> is LookupJoinRunner<VectorBatch*, VectorBatch*>
/**
 * IN: such as Object*, VectorBatch*
 * OUT: such as Object*, VectorBatch*
 * */
template <typename IN, typename OUT>
class ProcessOperator : public OneInputStreamOperator, public AbstractUdfStreamOperator<ProcessFunction<IN, OUT>, OUT> {
public:
    // it is called when the operator type is sql
    ProcessOperator(
        ProcessFunction<IN, OUT>* function, const nlohmann::json& description, Output* output, bool isStream = false)
        : AbstractUdfStreamOperator<ProcessFunction<IN, OUT>, OUT>(function)
    {
        this->output = output;
        this->isStream = isStream;
        this->chainingStrategy = ChainingStrategy::ALWAYS;
    };

    // it is called when the operator type is stream
    ProcessOperator(const nlohmann::json& description, Output* output, bool isStream = true)
    {
        LOG("-----create ProcessOperator-----");
        loadUdf(description);
        this->output = output;
        this->isStream = isStream;
    };

    ~ProcessOperator()
    {
        delete collector;
        delete context;
    }

    void loadUdf(nlohmann::json config)
    {
        std::string soPath = config["udf_so"];
        std::string udfObj = config["udf_obj"];
        nlohmann::json udfObjJson = nlohmann::json::parse(udfObj);
        LOG("ProcessOperator udf obj: " + udfObj);
        auto* symbol = udfLoader.LoadProcessOperatorFunction(soPath);
        if (symbol == nullptr) {
            throw std::out_of_range("null pointer when load " + soPath);
        }
        this->userFunction = symbol(udfObjJson).release();
    }

    void open() override
    {
        // ProcessFunction is opened here
        AbstractUdfStreamOperator<ProcessFunction<IN, OUT>, OUT>::open();
        collector = new TimestampedCollector(this->output, this->isStream);
        context = new ContextImpl(this->userFunction, this->getProcessingTimeService(), this);
    }

    void processElement(StreamRecord* element) override
    {
        collector->setTimestamp(element);
        context->element = element;
        auto value = reinterpret_cast<Object*>(element->getValue());
        this->userFunction->processElement(value, context, collector);
        value->putRefCount();
        context->element = nullptr;
    }

    void processBatch(StreamRecord* element) override
    {
        this->userFunction->processBatch(
            reinterpret_cast<omnistream::VectorBatch*>(element->getValue()), context, collector);
    }

    void initializeState(StreamTaskStateInitializerImpl* initializer, TypeSerializer* keySerializer) override
    {
        INFO_RELEASE("ProcessOperator::initializeState not impl initializeState");
    }

    void notifyCheckpointComplete(long checkpointId) override
    {
        INFO_RELEASE("ProcessOperator::notifyCheckpointComplete not impl checkpointId : " << checkpointId);
    }

    void notifyCheckpointAborted(long checkpointId) override
    {
        INFO_RELEASE("ProcessOperator::notifyCheckpointAborted not impl  checkpointId : " << checkpointId);
    }

    void ProcessWatermark(Watermark* mark) override
    {
        AbstractStreamOperator<OUT>::ProcessWatermark(mark);
        this->currentWatermark_ = mark->getTimestamp();
    }
    void processWatermarkStatus(WatermarkStatus* watermarkStatus) override
    {
        this->output->emitWatermarkStatus(watermarkStatus);
    }

    bool canBeStreamOperator() override
    {
        return this->isStream;
    }

private:
    class ContextImpl : public ProcessFunction<IN, OUT>::Context, public omnistream::streaming::TimerService {
    public:
        ContextImpl(ProcessFunction<IN, OUT>* function, ProcessingTimeService* timeService, ProcessOperator* op_)
            : processingTimeService(timeService),
              op(op_)
        {
            op->userFunction = function;
            reuse = new StreamRecord();
        }

        ~ContextImpl()
        {
            delete reuse;
        }

        int64_t timestamp() override
        {
            if (element->hasTimestamp()) {
                return element->getTimestamp();
            } else {
                // vanilla return null
                return 0;
            }
        }

        omnistream::streaming::TimerService* timerService() override
        {
            return this;
        }

        // sql scenario does not be considered,
        void Output(OutputTag* tag, Object* value) override
        {
            if (tag == nullptr) {
                THROW_LOGIC_EXCEPTION("OutputTag must not be null");
            }
            op->GetOutput()->collect(reuse->replace(value, element->getTimestamp()));
        }

        int64_t currentProcessingTime() override
        {
            return processingTimeService->getCurrentProcessingTime();
        }

        int64_t currentWatermark() override
        {
            return op->currentWatermark_;
        }

        void registerProcessingTimeTimer(int64_t time) override
        {
            NOT_IMPL_EXCEPTION;
        }
        void registerEventTimeTimer(int64_t time) override
        {
            NOT_IMPL_EXCEPTION;
        }

        void deleteProcessingTimeTimer(int64_t time) override
        {
            NOT_IMPL_EXCEPTION;
        }

        void deleteEventTimeTimer(int64_t time) override
        {
            NOT_IMPL_EXCEPTION;
        }

    public:
        StreamRecord* element = nullptr;
        ProcessingTimeService* processingTimeService = nullptr;
        ProcessOperator* op = nullptr;
        StreamRecord* reuse = nullptr;
    };

    TimestampedCollector* collector = nullptr;
    UDFLoader udfLoader;
    int64_t currentWatermark_ = std::numeric_limits<int64_t>::min();
    ContextImpl* context = nullptr;
};
#endif // FLINK_TNEL_PROCESSOPERATOR_H
