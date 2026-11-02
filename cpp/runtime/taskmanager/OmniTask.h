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

#ifndef OMNITASK_H
#define OMNITASK_H
#include <memory>
#include <mutex>
#include <executiongraph/JobInformationPOD.h>
#include <executiongraph/TaskInformationPOD.h>
#include <executiongraph/descriptor/TaskDeploymentDescriptorPOD.h>
#include <shuffle/ShuffleEnvironment.h>
#include <state/bridge/TaskStateManagerBridge.h>
#include <streaming/runtime/tasks/omni/OmniStreamTask.h>
#include "runtime/executiongraph/descriptor/ResultPartitionIDPOD.h"
#include "runtime/metrics/SizeGauge.h"
#include "runtime/metrics/groups/TaskMetricGroup.h"
#include "connector/kafka/bind_core_manager.h"
#include <state/bridge/OmniTaskBridge.h>

#include "io/network/netty/LocalNettyBufferPool.h"
#include "metrics/groups/TaskLocalNettyBufferMetricGroup.h"
#include "metrics/groups/VectorBatchBufferPoolMetricGroup.h"
#include "state/bridge/TaskOperatorEventGatewayBridge.h"
#include "runtime/buffer/OriginalNetworkBufferRecycler.h"
#include "runtime/partition/consumer/OmniLocalChannelReader.h"
#include <io/network/netty/OmniCreditBasedSequenceNumberingViewReader.h>
#include "partition/consumer/RemoteDataFetcherBridge.h"

namespace omnistream {
using AllocationID = AbstractIDPOD;

class OmniTask : public std::enable_shared_from_this<OmniTask> {
public:
    OmniTask(
        JobInformationPOD jobInfo,
        TaskInformationPOD taskInfo,
        TaskDeploymentDescriptorPOD taskDeploymentDescriptor,
        std::shared_ptr<ShuffleEnvironment> shuffleEnvironment,
        std::shared_ptr<TaskStateManagerBridge> stateBridge,
        std::shared_ptr<OmniTaskBridge> omni_task_bridge,
        std::shared_ptr<TaskOperatorEventGatewayBridge> taskOperatorEventGatewayBridge,
        std::shared_ptr<RemoteDataFetcherBridge> remoteDataFetcherBridge);

    OmniTask(
        JobInformationPOD jobInfo,
        TaskInformationPOD taskInfo,
        TaskDeploymentDescriptorPOD taskDeploymentDescriptor,
        std::shared_ptr<ShuffleEnvironment> shuffleEnvironment,
        std::shared_ptr<OmniTaskBridge> omni_task_bridge,
        std::shared_ptr<TaskOperatorEventGatewayBridge> taskOperatorEventGatewayBridge,
        std::shared_ptr<RemoteDataFetcherBridge> remoteDataFetcherBridge);

    OmniTask(
        std::shared_ptr<TaskStateManagerBridge> stateBridge,
        std::shared_ptr<OmniTaskBridge> omni_task_bridge,
        std::shared_ptr<TaskOperatorEventGatewayBridge> taskOperatorEventGatewayBridge,
        std::shared_ptr<RemoteDataFetcherBridge> remoteDataFetcherBridge);

    /** explicit OmniTask(ShuffleEnvironment *env)
     {
         NOT_IMPL_EXCEPTION;
     };
     */

    // Logs entry and exit so the teardown can be confirmed to have actually run, not merely started.
    ~OmniTask();

    [[nodiscard]] std::shared_ptr<RuntimeEnvironmentV2> getRuntimeEnv();

    // return ahd address of rawStreamTask
    uintptr_t setupStreamTask(std::string streamClassName);
    void DoRunRestore(long streamTaskAddress);
    void doRun(long streamTaskAddress);
    void DoRunInvoke(long streamTaskAddress);

    void cancel();
    void closeRocksDBSharedResources();
    ExecutionState getExecutionState();
    void setupPartitionsAndGates(
        std::vector<std::shared_ptr<ResultPartitionWriter>>& producedPartitions,
        std::vector<std::shared_ptr<SingleInputGate>>& inputGates);

    /**
     * Reports that this task's run loop has returned. May delete this task, so the caller must not
     * touch it afterwards. Returns true when the task was deleted.
     */
    bool NotifyRunFinished();

    void notifyRemoteDataAvailable(
        int inputGateIndex,
        int channelIndex,
        long bufferAddress,
        int bufferLength,
        int readIndex,
        int memorySegmentOffset,
        int sequenceNumber,
        bool isBuffer,
        int bufferType);

    long createNativeCreditBasedSequenceNumberingViewReader(
        long resultBufferAddress, ResultPartitionIDPOD partitionId, int subPartitionId, bool downStreamIsNative);
    void notifyCheckpointAborted(long checkpointid, long latestCompletedCheckpointId);
    void notifyCheckpointComplete(long checkpointID, long inputState);
    void notifyCheckpointSubsumed(long latestCompletedCheckpointId);

    void dispatchOperatorEvent(const std::string& operatorIdString, const std::string& eventString);
    void ReleaseResources();
    void CloseAllResultPartitions();
    void CloseAllInputGates();
    void FailAllResultPartitions();
    bool IsCanceledOrFailed();
    void triggerCheckpointBarrier(
        long checkpointId, long checkpointTimestamp, std::shared_ptr<CheckpointOptions> checkpointOptions);
    static constexpr const char* SOURCE_STREAM_TASK = "com.huawei.omniruntime.flink.runtime.tasks.OmniSourceStreamTask";
    static constexpr const char* SOURCE_OPERATOR_STREAM_TASK =
        "com.huawei.omniruntime.flink.runtime.tasks.OmniSourceOperatorStreamTask";
    static constexpr const char* ONEINTPUT_STREAM_TASK =
        "com.huawei.omniruntime.flink.runtime.tasks.OmniOneInputStreamTaskV2";
    static constexpr const char* TWOINTPUT_STREAM_TASK =
        "com.huawei.omniruntime.flink.runtime.tasks.OmniTwoInputStreamTaskV2";

    std::shared_ptr<TaskMetricGroup> getTaskMetricGroup();
    std::shared_ptr<TaskMetricGroup> createTaskMetricGroup();

    template <typename K>
    unsigned long CreateTask(std::shared_ptr<RuntimeEnvironmentV2> runtimeEnv);
    enum class NotifyCheckpointOperation {
        COMPLETE,
        ABORT,
        SUBSUME
    };
    void notifyCheckpoint(
        long checkpointid,
        long latestCompletedCheckpointId,
        OmniTask::NotifyCheckpointOperation notifyCheckpointOperation);
    void declineCheckpoint(long checkpointID, CheckpointFailureReason failureReason);
    void declineCheckpoint(long checkpointid, CheckpointFailureReason failureReason, std::exception* e);
    long createOmniLocalChannelReader(ResultPartitionIDPOD partitionId, int subPartitionId, long returnDataAddress);
    long changeLocalInputChannelToOriginal(ResultPartitionIDPOD partitionId);
    void notifyChannelToOmni(const ResultPartitionIDPOD& partitionId);
    int GetTaskType();
    long GetRecycleBufferAddress();
    std::shared_ptr<RemoteDataFetcherBridge> GetRemoteDataFetcherBridge();
    void SetTaskLocalNettyBufferMetricGroup(
        std::shared_ptr<TaskLocalNettyBufferMetricGroup> taskLocalNettyBufferMetricGroup);
    void SetVectorBatchBufferPoolMetricGroup(
        std::shared_ptr<VectorBatchBufferPoolMetricGroup> vectorBatchBufferPoolMetricGroup);
    SizeGauge::SizeSupplier CreateLocalNettyBufferMetricSupplier(const std::string& metricName);

private:
    std::atomic<bool> flag{false};
    JobInformationPOD jobInfo_;
    TaskInformationPOD taskInfo_;
    TaskDeploymentDescriptorPOD taskDeploymentDescriptor_;

    std::shared_ptr<ShuffleEnvironment> shuffleEnv_;
    std::shared_ptr<OmniStreamTask> invokable_;
    ;

    // internal
    TaskPlainInfoPOD taskPlainInfo_;

    //
    std::string taskNameWithSubtask_;

    /** The execution attempt of the parallel subtask. */
    ExecutionAttemptIDPOD executionId_;
    ExecutionState executionState = ExecutionState::CREATED;
    /** ID which identifies the slot in which the task is supposed to run. */
    AllocationID allocationId_;
    // shuffling
    std::shared_ptr<TaskStateManagerBridge> taskStateManagerBridge_;
    std::vector<std::shared_ptr<ResultPartitionWriter>> consumableNotifyingPartitionWriters;
    std::vector<std::shared_ptr<SingleInputGate>> inputGates;
    std::shared_ptr<TaskMetricGroup> taskMetricGroup;
    std::shared_ptr<RuntimeEnvironmentV2> runtimeEnv;
    std::shared_ptr<OmniTaskBridge> omni_task_bridge;
    std::shared_ptr<TaskOperatorEventGatewayBridge> taskOperatorEventGatewayBridge_;
    omnistream::BindCoreStrategy strategy = BindCoreStrategy::ALL_IN_ONE;
    int taskType;
    std::shared_ptr<OriginalNetworkBufferRecycler> originalNetworkBufferRecycler_ = nullptr;
    std::vector<std::unique_ptr<OmniLocalChannelReader>> omniLocalInputChannelReaders;
    std::vector<std::unique_ptr<OmniCreditBasedSequenceNumberingViewReader>>
        omniCreditBasedSequenceNumberingViewReaders;
    std::shared_ptr<RemoteDataFetcherBridge> remoteDataFetcherBridge_ = nullptr;
    std::vector<std::shared_ptr<LocalNettyBufferPool>> localNettyBufferPools;
    // Guards localNettyBufferPools: push_back runs concurrently on multiple netty-server threads
    // (one per partition request; 16 at parallelism 16), and the metric supplier iterates it.
    std::mutex localNettyBufferPoolsMutex_;
    std::shared_ptr<TaskLocalNettyBufferMetricGroup> taskLocalNettyBufferMetricGroup;
    std::shared_ptr<VectorBatchBufferPoolMetricGroup> vectorBatchBufferPoolMetricGroup;
};
} // namespace omnistream
#endif // OMNITASK_H
