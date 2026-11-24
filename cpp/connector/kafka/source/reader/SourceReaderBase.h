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

#ifndef FLINK_TNEL_SOURCEREADERBASE_H
#define FLINK_TNEL_SOURCEREADERBASE_H

#include "core/api/connector/source/SourceReader.h"
#include "connector/kafka/source/reader/synchronization/FutureCompletingBlockingQueue.h"
#include "core/api/connector/source/SourceReaderContext.h"
#include "connector/kafka/source/reader/fetcher/KafkaSourceFetcherManager.h"
#include "connector/kafka/source/reader/KafkaRecordEmitter.h"
#include "connector/kafka/source/reader/KafkaPartitionSplitReader.h"
#include "SplitContext.h"
#include "connector/kafka/source/split/KafkaPartitionSplitState.h"
#include "connector/kafka/bind_core_manager.h"
#include "core/include/common.h"
#include <algorithm>
#include <climits>

template <typename E, typename SplitT, typename SplitStateT>
class SourceReaderBase : public SourceReader<SplitT> {
public:
    SourceReaderBase(
        FutureCompletingBlockingQueue<RdKafka::Message>* elementsQueue,
        SingleThreadFetcherManager<E, SplitT>* splitFetcherManager,
        RecordEmitter<E, SplitStateT>* recordEmitter,
        SourceReaderContext* context,
        bool isBatch)
        : isBatch(isBatch),
          elementsQueue(elementsQueue),
          splitStates(),
          recordEmitter(recordEmitter),
          splitFetcherManager(splitFetcherManager),
          context(context),
          noMoreSplitsAssignment(false)
    {
        if (isBatch) {
            reuseMessageVec.reserve(batchSize);
        }
        currentSplitContext = nullptr;
        currentSplitOutput = nullptr;
    }

    virtual ~SourceReaderBase()
    {
        // This batch is no longer in the queue, but still holds Kafka references.
        delete currentFetch;
        currentFetch = nullptr;
        // Join fetchers before releasing the queue they still use during shutdown.
        delete splitFetcherManager;
        delete elementsQueue;
        delete recordEmitter;
        delete context;
        for (auto& item : splitStates) {
            delete item.second;
        }
        splitStates.clear();
    }

    // 轮询下一个记录
    InputStatus pollNext(ReaderOutput* output) override
    {
        RecordsWithSplitIds<E>* recordsWithSplitId = currentFetch;
        if (!recordsWithSplitId) {
            recordsWithSplitId = getNextFetch(output);
            if (!recordsWithSplitId) {
                return trace(finishedOrAvailableLater());
            }
        }

        // emit all records and one batch per pollNext, which is better
        const std::vector<RdKafka::Message*>& records = recordsWithSplitId->getRecordsFromSplit();
        if (isBatch) {
            batchPoll(records, recordsWithSplitId);
        } else {
            normalPoll(records, recordsWithSplitId);
        }
        moveToNextSplit(recordsWithSplitId, output);
        return InputStatus::MORE_AVAILABLE;
    }

    void normalPoll(const std::vector<RdKafka::Message*>& records, RecordsWithSplitIds<E>* recordsWithSplitId)
    {
        auto currentSplitStoppingOffset = recordsWithSplitId->getSplitStoppingOffset();
        for (RdKafka::Message* record : records) {
            if (record->offset() >= currentSplitStoppingOffset) {
                break;
            }
            recordEmitter->emitRecord(record, currentSplitOutput, currentSplitContext->state);
        }
    }

    void batchPoll(const std::vector<RdKafka::Message*>& records, RecordsWithSplitIds<E>* recordsWithSplitId)
    {
        auto currentSplitStoppingOffset = recordsWithSplitId->getSplitStoppingOffset();
        size_t totalRecords = records.size();
        for (size_t i = 0; i < totalRecords; i += batchSize) {
            reuseMessageVec.clear();
            size_t currentGroupSize = std::min(batchSize, totalRecords - i);
            // 将当前组的元素添加到 messageVec
            for (size_t j = 0; j < currentGroupSize; ++j) {
                if (static_cast<size_t>(records[i + j]->offset()) >= currentSplitStoppingOffset) {
                    break;
                }
                reuseMessageVec.push_back(records[i + j]);
            }
            if (reuseMessageVec.empty()) {
                break;
            }
            // 处理当前组
            recordEmitter->emitBatchRecord(reuseMessageVec, currentSplitOutput, currentSplitContext->state);
            if (reuseMessageVec.size() < batchSize) {
                break;
            }
        }
    }

    std::shared_ptr<omnistream::CompletableFuture> getAvailable() override
    {
        if (currentFetch != nullptr) {
            return FutureCompletingBlockingQueue<RdKafka::Message>::AVAILABLE;
        } else {
            return elementsQueue->getAvailabilityFuture();
        }
    }

    // 对拆分状态进行快照
    std::vector<KafkaPartitionSplit> snapshotState(long checkpointId) override
    {
        (void)checkpointId;
        std::vector<KafkaPartitionSplit> splits;
        for (const auto& [splitId, splitContext] : this->splitStates) {
            if (splitContext->state->getCurrentOffset() >= 0) {
                KafkaPartitionSplit split = splitContext->state->toKafkaPartitionSplit();
                splits.push_back(split);
            }
        }
        return splits;
    }

    // 通知检查点完成
    void notifyCheckpointComplete(long checkpointId) override
    {
        LOG("SourceReaderBase::notifyCheckpointComplete not impl, checkpointId : " << checkpointId);
    }

    // 通知检查点完成
    void notifyCheckpointAborted(long checkpointId) override
    {
        LOG("SourceReaderBase::notifyCheckpointAborted not impl, checkpointId : " << checkpointId);
    }

    // 添加拆分
    void addSplits(std::vector<SplitT*>& splits) override
    {
        if (splitFetcherManager == nullptr) {
            THROW_RUNTIME_ERROR("SourceReaderBase addSplits called with null splitFetcherManager.");
        }

        std::vector<SplitT*> splitsToAdd;
        splitsToAdd.reserve(splits.size());

        // Initialize the state for each new split.
        for (const auto& s : splits) {
            if (s == nullptr) {
                continue;
            }

            const std::string splitId = s->splitId();
            if (splitStates.find(splitId) != splitStates.end()) {
                continue;
            }

            splitStates.emplace(splitId, new SplitContext(splitId, initializedState(s)));
            splitsToAdd.push_back(s);
        }

        // Hand over only new splits to the split fetcher to start fetch.
        if (!splitsToAdd.empty()) {
            splitFetcherManager->addSplits(splitsToAdd);
        }
    }

    // 通知没有更多拆分
    void notifyNoMoreSplits() override
    {
        LOG("Reader received NoMoreSplits event.");
        noMoreSplitsAssignment = true;
        if (elementsQueue) {
            elementsQueue->notifyAvailable();
        }
    }
    // 处理源事件
    void handleSourceEvents(const SourceEvent& sourceEvent) override
    {
        LOG("Received unhandled source event.");
    }

    void start() override
    {
    }

    // 关闭读取器
    void close() override
    {
        LOG("Closing Source Reader");
        splitFetcherManager->close(30000);
    }

    // 获取当前分配的拆分数量
    int getNumberOfCurrentlyAssignedSplits() const
    {
        return static_cast<int>(splitStates.size());
    }

protected:
    // 初始化新拆分的状态
    virtual KafkaPartitionSplitState* initializedState(KafkaPartitionSplit* split) = 0;

    virtual void onSplitFinished(const std::unordered_map<std::string, KafkaPartitionSplitState*>& finishedSplitIds) {};
    SplitFetcherManager<E, SplitT>* splitFetcherManager;

private:
    bool isBatch;
    FutureCompletingBlockingQueue<E>* elementsQueue;
    std::unordered_map<std::string, SplitContext*> splitStates;
    RecordEmitter<E, SplitStateT>* recordEmitter;
    SourceReaderContext* context;
    // 最新从拆分读取器提取的按拆分的记录批次
    RecordsWithSplitIds<E>* currentFetch = nullptr;
    // 当前拆分的上下文
    SplitContext* currentSplitContext;
    // 当前拆分的输出
    SourceOutput* currentSplitOutput;
    // 指示 SourceReader 是否将被分配更多拆分
    bool noMoreSplitsAssignment;

    size_t batchSize = 1000;

    int32_t bindCore1 = -1;

    int32_t bindCore2 = -1;

    bool binded1 = false, binded2 = false;

    std::vector<RdKafka::Message*> reuseMessageVec;

    // 跟踪输入状态并记录日志
    InputStatus trace(InputStatus status)
    {
        return status;
    }
    // 获取下一个提取的记录批次
    RecordsWithSplitIds<E>* getNextFetch(ReaderOutput* output)
    {
        splitFetcherManager->checkErrors();
        RecordsWithSplitIds<E>* recordsWithSplitId = elementsQueue->poll();
        if (!recordsWithSplitId || !moveToNextSplit(recordsWithSplitId, output)) {
            return nullptr;
        }
        currentFetch = recordsWithSplitId;
        return recordsWithSplitId;
    }
    // 完成当前提取
    void finishCurrentFetch(RecordsWithSplitIds<E>* fetch, ReaderOutput* output)
    {
        currentFetch = nullptr;
        currentSplitContext = nullptr;
        currentSplitOutput = nullptr;

        if (unlikely(not binded1)) {
            if (elementsQueue->getCoreId() >= 0) {
                omnistream::BindCoreManager::GetInstance()->BindDirectCore(elementsQueue->getCoreId());
            }
            binded1 = true;
        }

        const std::set<std::string>& finishedSplits = fetch->finishedSplits();
        if (!finishedSplits.empty()) {
            std::unordered_map<std::string, KafkaPartitionSplitState*> stateOfFinishedSplits;
            for (const auto& finishedSplitId : finishedSplits) {
                stateOfFinishedSplits[finishedSplitId] = splitStates[finishedSplitId]->state;
                output->ReleaseOutputForSplit(finishedSplitId);
            }
            onSplitFinished(stateOfFinishedSplits);
            for (const auto& finishedSplitId : finishedSplits) {
                SplitContext* splitContext = splitStates[finishedSplitId];
                delete splitContext;
                splitStates.erase(finishedSplitId);
            }
        }

        fetch->recycle();
        // todo delete
        delete fetch;
    }
    // 移动到下一个拆分
    bool moveToNextSplit(RecordsWithSplitIds<E>* recordsWithSplitIds, ReaderOutput* output)
    {
        const std::string& nextSplitId = recordsWithSplitIds->nextSplit();
        if (nextSplitId.empty()) {
            finishCurrentFetch(recordsWithSplitIds, output);
            return false;
        }
        auto it = splitStates.find(nextSplitId);
        if (it == splitStates.end()) {
            return false;
            // throw std::runtime_error("Have records for a split that was not registered");
        }
        currentSplitContext = it->second;
        currentSplitOutput = &(currentSplitContext->getOrCreateSplitOutput(output));
        return true;
    }

    // 判断读取是否结束或稍后可用
    InputStatus finishedOrAvailableLater()
    {
        bool allFetchersHaveShutdown = splitFetcherManager->maybeShutdownFinishedFetchers();
        if (!(noMoreSplitsAssignment && allFetchersHaveShutdown)) {
            return InputStatus::NOTHING_AVAILABLE;
        }
        if (elementsQueue->isEmpty()) {
            // We may reach here because of exceptional split fetcher, check it.
            splitFetcherManager->checkErrors();
            return InputStatus::END_OF_INPUT;
        } else {
            // We can reach this case if we just processed all data from the queue and finished a
            // split,
            // and concurrently the fetcher finished another split, whose data is then in the queue.
            return InputStatus::MORE_AVAILABLE;
        }
    }
};
#endif // FLINK_TNEL_SOURCEREADERBASE_H
