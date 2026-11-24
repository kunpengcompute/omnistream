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

#include <string>
#include <cstdint>
#include <nlohmann/json.hpp>
#include <vector> // Include the vector header
#include <iostream>
#include <fstream>
#include "common.h"
#include "runtime/io/network/api/writer/RecordWriter.h"
#include "../../streaming/api/operators/StreamOperator.h"
#include "outputbuffer.h"
#include "../graph/TaskPartitionerConfig.h"
#include "streaming/runtime/io/StreamTaskNetworkOutput.h"
#include "streaming/runtime/io/StreamTaskNetworkInput.h"
#include "../../streaming/runtime/io/StreamOneInputProcessor.h"
#include "taskmanager/OmniRuntimeEnvironment.h"
#include "streaming/runtime/partitioner/V2/StreamPartitionerV2.h"
#include "table/runtime/keyselector/KeySelector.h"
#include "OperatorChain.h"

using json = nlohmann::json;
// class StreamOneInputProcessor;

namespace omnistream {
class OperatorChainV2;
}

namespace omnistream::datastream {

using ::omnistream::OperatorChainV2;

class StreamTask {
public:
    StreamTask(
        const nlohmann::json& ntdd,
        void* outputBufferStatus,
        std::shared_ptr<RuntimeEnvironmentV2> runtimeEnv); // Default constructor

    ~StreamTask(); // Destructor
    void cleanUp();

    // entry for native
    StreamTaskStateInitializerImpl* createStreamTaskStateInitializer();

    uint32_t emitNextRecord();

    void clearOutputBuffer();

    OutputBufferStatus* getOutputBufferStatus();

    StreamOneInputProcessor* createOmniInputProcessor(const json& channelInfo, int operatorMethodIndicator);

    void addStreamOneInputProcessor(StreamOneInputProcessor* processor);

    StreamOneInputProcessor* getStreamOneInputProcessor(size_t index);

    void resetOutputBufferAndRecordWriter();

    StreamOperator* getMainOperator()
    {
        return mainOperator_;
    }

    omnistream::datastream::StreamPartitioner<IOReadableWritable>* createPartitioner();

    static TaskPartitionerConfig extractTaskPartitionerConfig(const json& ntdd);
    void setTaskPartitionerConfig(TaskPartitionerConfig taskPartitionerConfig_)
    {
        taskPartitionerConfig = taskPartitionerConfig_;
    }

private:
    TaskPartitionerConfig taskPartitionerConfig;

    // weak ref, the objet is owned by java object
    OutputBufferStatus* outputBufferStatus_;

    // owning
    std::shared_ptr<RuntimeEnvironmentV2> env_;
    std::vector<OperatorConfig> operatorChainConfig_;

    // need read some more code.
    std::shared_ptr<RecordWriterDelegateV2> recordWriter_;
    // owning
    OperatorChainV2* operatorChain_;

    // weal ref. mainOperator_ is only a shortcut of first operator in operator chain. should be deleted by
    // operatroChain_
    StreamOperator* mainOperator_;

    // owning, although some mem function return the pointer to outside of class
    std::vector<StreamOneInputProcessor*> streamOneInputProcessors;

    // set up ////////////////////////////////////////set up
    // ntdd  Native task deployment descriptor
    // return int error code, 0 is for success
    int createTask(const json& ntdd);

    // convert the json ntdd to object ndd
    static int extractOperatorChainConfig(std::vector<OperatorConfig>&, const json& ntdd);

    int setupRecordWriter();
    int setupOperatorChain(std::vector<OperatorConfig>&);
    std::unique_ptr<StreamTaskNetworkOutput> createDataOutput(int operatorMethodIndicator);
    std::unique_ptr<StreamTaskNetworkInput> createDataInput(std::vector<long>& channelInfos);

    // record writer
    std::shared_ptr<RecordWriterDelegateV2> createRecordWriterDelegate(
        TaskInformationPOD taskConfig, std::shared_ptr<RuntimeEnvironmentV2> environment);

    std::vector<RecordWriterV2*> createRecordWriters(
        TaskInformationPOD taskConfig, std::shared_ptr<RuntimeEnvironmentV2> environment);
    RecordWriterV2* createRecordWriter(
        StreamEdgePOD& edge,
        int outputIndex,
        std::shared_ptr<RuntimeEnvironmentV2> environment,
        std::string taskName,
        long bufferTimeout);

    template <typename K>
    KeySelector<K>* buildKeySelector(std::vector<KeyFieldInfoPOD>& keyFields);

    // partitioner
    StreamPartitioner<IOReadableWritable>* createPartitionerFromDesc(const StreamEdgePOD& edge);
};
} // namespace omnistream::datastream
