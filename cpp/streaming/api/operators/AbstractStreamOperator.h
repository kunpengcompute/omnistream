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

#pragma once

#include <cstdint>
#include <memory>
#include <nlohmann/json.hpp>
#include "StreamOperator.h"
#include "NamedOperator.h"
#include "StreamOperatorStateHandler.h"
#include "Output.h"
#include "StreamingRuntimeContext.h"
#include "StreamTaskStateInitializerImpl.h"
#include "ChainingStrategy.h"
#include "Input.h"
#include "table/runtime/operators/InternalTimerServiceImpl.h"
#include "table/runtime/operators/InternalTimeServiceManager.h"
#include "KeyContext.h"
#include "streaming/runtime/tasks/ProcessingTimeService.h"
#include "core/api/common/eventtime/IndexedCombinedWatermarkStatus.h"
#include "runtime/metrics/groups/TaskMetricGroup.h"
#include "runtime/state/StateInitializationContextImpl.h"
#include "runtime/checkpoint/FlinkSavepointAdaptorInfo.h"

class Object;
class RowData;

template <typename K>
class StreamingRuntimeContext;

namespace omnistream {
class OmniStreamTask;
class VectorBatch;
} // namespace omnistream

class Object;
class RowData;

template <typename K>
class StreamingRuntimeContext;

namespace omnistream {
class OmniStreamTask;
class VectorBatch;
} // namespace omnistream

/**
 * K: such as Object*
 * */
template <typename K>
class AbstractStreamOperator : public StreamOperator,
                               public KeyContext<K>,
                               public NamedOperator,
                               public StreamOperatorStateHandler<K>::CheckpointedStreamOperator {
public:
    void setDescription(nlohmann::json description)
    {
        desc = description;
    }

    AbstractStreamOperator()
    {
        this->runtimeContext = nullptr;
    }

    explicit AbstractStreamOperator(Output* output)
    {
        this->output = output;
        this->runtimeContext = nullptr;
    }

    ~AbstractStreamOperator() override;

    void setup();

    void setup(std::shared_ptr<omnistream::OmniStreamTask> task);

    std::shared_ptr<omnistream::TaskMetricGroup> GetMectrics() override
    {
        LOG("AbstractStreamOperator GetMectrics");
        return this->metrics;
    }

    void setOutput(Output* outputPtr)
    {
        this->output = outputPtr;
    }

    Output* GetOutput()
    {
        return output;
    }

    void setCurrentKey(K key) override;

    K getCurrentKey() override;

    void open() override {};

    void close() override;

    TypeSerializer* GetOperatorKeySerializer();

    void initializeState(StateInitializationContextImpl* context) override
    {
    }

    FlinkSavepointAdaptorInfo getSavepointAdaptorInfo() const override
    {
        return flinkSavepointAdaptorInfo_;
    }

    nlohmann::json getOperatorDescription() const override
    {
        return desc;
    }

    // KeySerializer should be retrieved from description.getStateKeySerializer(getUserCodeClassloader()),
    // but we're just passing it through this function for now
    void initializeState(StreamTaskStateInitializerImpl* initializer, TypeSerializer* keySerializer) override;
    StreamingRuntimeContext<K>* getRuntimeContext() const
    {
        return runtimeContext;
    }

    AbstractKeyedStateBackend<K>* getKeyedStateBackend() const;

    OperatorStateBackend* getOperatorStateBackend();

    std::string getTypeName() override
    {
        std::string typeName = "AbstractStreamOperator";
        typeName.append(__PRETTY_FUNCTION__);
        return typeName;
    }
    template <typename N>
    InternalTimerServiceImpl<K, N>* getInternalTimerService(
        std::string name, TypeSerializer* namespaceSerializer, Triggerable<K, N>* triggerable)
    {
        if (timeServiceManager == nullptr) {
            THROW_LOGIC_EXCEPTION("The timer service has not been initialized");
        }
        AbstractKeyedStateBackend<K>* keyedStateBackend = getKeyedStateBackend();
        if (keyedStateBackend == nullptr) {
            THROW_LOGIC_EXCEPTION("Timers can only be used on keyed operators");
        }
        return timeServiceManager->template getInternalTimerService<N>(
            name, keyedStateBackend->getKeySerializer(), namespaceSerializer, triggerable);
    }

    virtual void ProcessWatermark(Watermark* mark)
    {
        if (timeServiceManager != nullptr) {
            timeServiceManager->advanceWatermark(mark);
        }
        output->emitWatermark(mark);
    }

    void ProcessWatermark1(Watermark* mark)
    {
        ProcessWatermark(mark, 0);
    }

    void ProcessWatermark2(Watermark* mark)
    {
        ProcessWatermark(mark, 1);
    }
    void processWatermarkStatus(WatermarkStatus* watermarkStatus)
    {
        output->emitWatermarkStatus(watermarkStatus);
    }

    void processWatermarkStatus1(WatermarkStatus* watermarkStatus)
    {
        processWatermarkStatus(watermarkStatus, 0);
    }

    void processWatermarkStatus2(WatermarkStatus* watermarkStatus)
    {
        processWatermarkStatus(watermarkStatus, 1);
    }

    void setProcessingTimeService(ProcessingTimeService* service)
    {
        processingTimeService = service;
    };

    ProcessingTimeService* getProcessingTimeService()
    {
        return processingTimeService;
    };

    OperatorSnapshotFutures* SnapshotState(
        long checkpointId,
        long timestamp,
        CheckpointOptions* checkpointOptions,
        std::shared_ptr<CheckpointStreamFactory> storageLocation,
        const std::shared_ptr<OmniTaskBridge>& bridge) override;

    void notifyCheckpointComplete(long checkpointId) override;

    void notifyCheckpointAborted(long checkpointId) override;

    // 设置具体 Adaptor 类型（或 OmniIsCompatible）。禁止用于表达 None —— None 必须走
    // setFlinkSavepointUnsupported(reason) 携带可诊断原因。
    void setFlinkSavepointAdaptor(FlinkSavepointAdaptorType type)
    {
        if (type == FlinkSavepointAdaptorType::None) {
            INFO_RELEASE(
                "Error: setFlinkSavepointAdaptor called with None, "
                "use setFlinkSavepointUnsupported(reason) for unsupported operators");
            throw std::invalid_argument("Use setFlinkSavepointUnsupported(reason) for unsupported operators");
        }
        flinkSavepointAdaptorInfo_ = {type, ""};
    }

    // 设置 None + 可诊断 reason，表达该算子不支持 compatible savepoint。
    void setFlinkSavepointUnsupported(std::string reason)
    {
        if (reason.empty()) {
            INFO_RELEASE("Error: Unsupported compatible savepoint adaptor reason must not be empty");
            throw std::invalid_argument("Unsupported compatible savepoint adaptor reason must not be empty");
        }
        flinkSavepointAdaptorInfo_ = {FlinkSavepointAdaptorType::None, std::move(reason)};
    }

protected:
    // own  and  own the backend through stateHandler
    StreamOperatorStateHandler<K>* stateHandler = nullptr;

    Output* output = nullptr;
    // should not own the backend though runtimeContext
    StreamingRuntimeContext<K>* runtimeContext = nullptr;
    ChainingStrategy chainingStrategy;
    nlohmann::json desc;
    InternalTimeServiceManager<K>* timeServiceManager = nullptr;
    std::shared_ptr<omnistream::TaskMetricGroup> metrics;
    bool isStream = false;
    omnistream::IndexedCombinedWatermarkStatus* combinedWatermark = nullptr;

private:
    void processWatermarkStatus(WatermarkStatus* watermarkStatus, int index)
    {
        bool wasIdle = combinedWatermark->IsIdle();
        if (combinedWatermark->UpdateStatus(index, watermarkStatus->IsIdle())) {
            Watermark watermark(combinedWatermark->GetCombinedWatermark());
            this->ProcessWatermark(&watermark);
        }
        if (wasIdle != combinedWatermark->IsIdle()) {
            output->emitWatermarkStatus(watermarkStatus);
        }
    }

    ProcessingTimeService* processingTimeService = nullptr;
    FlinkSavepointAdaptorInfo flinkSavepointAdaptorInfo_{
        FlinkSavepointAdaptorType::None, "No compatible savepoint adaptor has been assigned"};

    void ProcessWatermark(Watermark* mark, int index)
    {
        LOG(">>>>>>>>>>");
        if (combinedWatermark->UpdateWatermark(index, mark->getTimestamp())) {
            Watermark watermark(combinedWatermark->GetCombinedWatermark());
            this->ProcessWatermark(&watermark);
        }
    }
};

extern template class AbstractStreamOperator<int32_t>;
extern template class AbstractStreamOperator<int64_t>;
extern template class AbstractStreamOperator<void*>;
extern template class AbstractStreamOperator<Object*>;
extern template class AbstractStreamOperator<RowData*>;
extern template class AbstractStreamOperator<std::shared_ptr<RowData>>;
extern template class AbstractStreamOperator<omnistream::VectorBatch*>;
