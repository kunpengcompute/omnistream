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
#ifndef FLINK_TNEL_ABSTRACTUDFSTREAMOPERATOR_H
#define FLINK_TNEL_ABSTRACTUDFSTREAMOPERATOR_H

#include "streaming/api/operators/AbstractStreamOperator.h"
#include "functions/AbstractRichFunction.h"
#include "streaming/util/functions/StreamingFunctionUtils.h"

// Class should also contain the following if needed
// <OUT, F extends Function>
// OUT is the output type of the operator, the DataView requires it
// F is the UDF initialized through codegen
template <typename F, typename K>
class AbstractUdfStreamOperator : public AbstractStreamOperator<K> {
public:
    AbstractUdfStreamOperator() = default;
    explicit AbstractUdfStreamOperator(F* userFunction) : userFunction(userFunction)
    {
    }
    AbstractUdfStreamOperator(F* userFunction, Output* output)
        : AbstractStreamOperator<K>(output),
          userFunction(userFunction)
    {
    }

    ~AbstractUdfStreamOperator()
    {
        delete userFunction;
    };

    F* getUserFunction()
    {
        return userFunction;
    }

    void setup()
    {
        AbstractStreamOperator<K>::setup();
        auto richFunctionPtr = dynamic_cast<RichFunction*>(userFunction);
        if (richFunctionPtr != nullptr) {
            richFunctionPtr->setRuntimeContext(this->runtimeContext);
        }
    }

    void setup(std::shared_ptr<omnistream::OmniStreamTask> task)
    {
        AbstractStreamOperator<K>::setup(std::move(task));
        auto richFunctionPtr = dynamic_cast<RichFunction*>(userFunction);
        if (richFunctionPtr != nullptr) {
            richFunctionPtr->setRuntimeContext(this->runtimeContext);
        }
    }

    void open() override
    {
        AbstractStreamOperator<K>::open();
        auto richFunctionPtr = dynamic_cast<RichFunction*>(userFunction);
        if (richFunctionPtr != nullptr) {
            richFunctionPtr->open(Configuration());
        }
    }

    std::string getTypeName() override
    {
        std::string typeName = "AbstractUdfStreamOperator";
        typeName.append(__PRETTY_FUNCTION__);
        return typeName;
    }

    void notifyCheckpointComplete(long checkpointId)
    {
        AbstractStreamOperator<K>::notifyCheckpointComplete(checkpointId);
        if (auto uf = dynamic_cast<CheckpointListener*>(userFunction)) {
            uf->notifyCheckpointComplete(checkpointId);
        }
    }

    void notifyCheckpointAborted(long checkpointId)
    {
        AbstractStreamOperator<K>::notifyCheckpointAborted(checkpointId);
        if (auto uf = dynamic_cast<CheckpointListener*>(userFunction)) {
            uf->notifyCheckpointAborted(checkpointId);
        }
    }

    void close() override
    {
        AbstractStreamOperator<K>::close();
        if (auto* richFunction = dynamic_cast<RichFunction*>(userFunction)) {
            richFunction->close();
        }
    }

    void initializeState(StateInitializationContextImpl* context) override
    {
        AbstractStreamOperator<K>::initializeState(context);
        StreamingFunctionUtils::restoreFunctionState(context, userFunction);
    }

    void initializeState(StreamTaskStateInitializerImpl* initializer, TypeSerializer* keySerializer) override
    {
        AbstractStreamOperator<K>::initializeState(initializer, keySerializer);
    }

    void snapshotState(StateSnapshotContextSynchronousImpl* context) override
    {
        AbstractStreamOperator<K>::snapshotState(context);
        StreamingFunctionUtils::snapshotFunctionState(context, getOperatorStateBackend(), userFunction);
    }

protected:
    F* userFunction = nullptr;
};

#endif // FLINK_TNEL_ABSTRACTUDFSTREAMOPERATOR_H
