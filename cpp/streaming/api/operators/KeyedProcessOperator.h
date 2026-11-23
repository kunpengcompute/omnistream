/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */
#ifndef FLINK_TNEL_KEYED_PROCESS_OPERATOR_H
#define FLINK_TNEL_KEYED_PROCESS_OPERATOR_H

#include "ChainingStrategy.h"
#include "streaming/api/operators/AbstractUdfStreamOperator.h"
#include "streaming/api/operators/OneInputStreamOperator.h"
#include "TimestampedCollector.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "runtime/state/VoidNamespace.h"
#include "streaming/api/functions/KeyedProcessFunction.h"
#include "table/runtime/operators/aggregate/GroupAggFunction.h"
#include "runtime/operators/rank/AbstractTopNFunction.h"
#include "runtime/operators/rank/FastTop1Function.h"

// K => Key Type, IN => Input Type, OUT => Output Type

template <typename K, typename IN, typename OUT>
class ContextImpl;

template <typename K, typename IN, typename OUT>
class KeyedProcessOperator : public AbstractUdfStreamOperator<KeyedProcessFunction<K, IN, OUT>, K>,
                             public OneInputStreamOperator {
public:
    using F = KeyedProcessFunction<K, IN, OUT>;

    KeyedProcessOperator(F* function, Output* output, nlohmann::json desc) : AbstractUdfStreamOperator<F, K>(function)
    {
        this->chainingStrategy = ChainingStrategy::ALWAYS;
        this->output = output;
        if (desc.contains("partitionKey")) {
            this->keyedIndex = desc.at("partitionKey").get<std::vector<int>>();
        } else {
            this->keyedIndex = desc["grouping"].get<std::vector<int32_t>>();
        }
    }

    ~KeyedProcessOperator() override
    {
        // open() allocates both; nothing else owns them, and the task destroys this operator
        // at the end of each job.
        delete collector;
        collector = nullptr;
        delete context;
        context = nullptr;
    };

    void open() override
    {
        AbstractUdfStreamOperator<F, K>::open();
        collector = new TimestampedCollector(this->output);
        context = new ContextImpl<K, IN, OUT>(this->userFunction, this);
        // userFunction has already been opened in AbstractUdfStreamFunction's open
        reUseKeyRow = BinaryRowData::createBinaryRowDataWithMem(keyedIndex.size());
    }

    void close() override
    {
        AbstractUdfStreamOperator<F, K>::close();
    }

    JoinedRowData* getResultRow()
    {
        return this->userFunction->getResultRow();
    }
    void ProcessWatermark(Watermark* watermark) override
    {
        AbstractStreamOperator<K>::ProcessWatermark(watermark);
    }
    void processElement(StreamRecord* element) override
    {
        collector->setTimestamp(element);
        if (context->element != nullptr) {
            delete context->element;
        }
        context->element = element;
        // In sql API, all input StreamRecord for operator is RowData*
        this->userFunction->processElement(static_cast<IN>(element->getValue()), context, collector); // GroupAgg
        context->element = nullptr;
    }

    void processBatch(StreamRecord* element) override
    {
        auto elementGuard = std::unique_ptr<StreamRecord>(element);
        LOG("KeyedProcessOperator processBatch running");
        this->userFunction->processBatch(
            reinterpret_cast<omnistream::VectorBatch*>(element->getValue()), *context, *collector);
        LOG("KeyedProcessOperator processBatch end");
    }

    void initializeState(StreamTaskStateInitializerImpl* initializer, TypeSerializer* keySerializer) override
    {
        INFO_RELEASE(
            "savepoint: KeyedProcessOperator initializeState with initializer, operatorID: "
            << OneInputStreamOperator::GetOperatorID().toString());
        AbstractStreamOperator<K>::SetOperatorID(OneInputStreamOperator::GetOperatorID().toString());
        AbstractStreamOperator<K>::initializeState(initializer, keySerializer);
        // Operator specifig initialization
    }

    void notifyCheckpointComplete(long checkpointId) override
    {
        AbstractUdfStreamOperator<F, K>::notifyCheckpointComplete(checkpointId);
    }

    void notifyCheckpointAborted(long checkpointId) override
    {
        AbstractUdfStreamOperator<F, K>::notifyCheckpointAborted(checkpointId);
    }

    bool isSetKeyContextElement() override
    {
        return true;
    }
    void setKeyContextElement(StreamRecord* record)
    {
        for (size_t i = 0; i < keyedIndex.size(); ++i) {
            int64_t keyVal = *(reinterpret_cast<RowData*>(record->getValue())->getLong(keyedIndex[i]));
            reUseKeyRow->setLong(i, keyVal);
        }
        // Check if K is RowData*
        if constexpr (std::is_same_v<K, RowData*>) {
            this->setCurrentKey(reUseKeyRow);
        }
    }

    const char* getName() override
    {
        return "KeyedProcessOperator";
    }

    std::string getTypeName() override
    {
        std::string typeName = "KeyedProcessOperator";
        typeName.append(__PRETTY_FUNCTION__);
        return typeName;
    }

    void processWatermarkStatus(WatermarkStatus* watermarkStatus) override
    {
        this->output->emitWatermarkStatus(watermarkStatus);
    }

private:
    // Null until open() runs -- open() can throw partway, and the destructor frees these.
    TimestampedCollector* collector = nullptr;
    ContextImpl<K, IN, OUT>* context = nullptr;
    std::vector<int32_t> keyedIndex;
    BinaryRowData* reUseKeyRow;
};

template <typename K, typename IN, typename OUT>
class ContextImpl : public KeyedProcessFunction<K, IN, OUT>::Context {
public:
    ContextImpl(KeyedProcessFunction<K, IN, OUT>* function, KeyedProcessOperator<K, IN, OUT>* owner)
        : KeyedProcessFunction<K, IN, OUT>::Context(),
          owner_(owner)
    {
    }
    long timestamp() const override
    {
        // NOT IMPLEMENTED
        return 0;
    }

    omnistream::streaming::TimerService* timerService() override
    {
        return localTimerService.get();
    }

    K getCurrentKey() const override
    {
        return owner_->getCurrentKey();
    }
    void setCurrentKey(K key) override
    {
        owner_->setCurrentKey(key);
    }
    StreamRecord* element = nullptr;

private:
    std::shared_ptr<omnistream::streaming::TimerService> localTimerService;
    KeyedProcessOperator<K, IN, OUT>* owner_;
};

template <typename K, typename IN, typename OUT>
class OnTimerContextImpl : public KeyedProcessFunction<K, IN, OUT>::OnTimerContext {
    // NOT IMPLEMENTED
};

#endif // FLINK_TNEL_KEYED_PROCESS_OPERATOR_H
