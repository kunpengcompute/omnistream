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

#ifndef OMNISTREAM_OPERATORSNAPSHOTFUTURES
#define OMNISTREAM_OPERATORSNAPSHOTFUTURES

#include <condition_variable>
#include <future>
#include <memory>
#include <mutex>
#include <stdexcept>
#include "runtime/state/SnapshotResult.h"
#include "runtime/state/KeyedStateHandle.h"
#include "runtime/state/OperatorStateHandle.h"
#include "runtime/checkpoint/StateObjectCollection.h"
#include "runtime/state/InputChannelStateHandle.h"
#include "runtime/state/ResultSubpartitionStateHandle.h"

class OperatorSnapshotFutures {
public:
    explicit OperatorSnapshotFutures()
    {
        keyedStateManagedFuture = nullptr;
        keyedStateRawFuture = nullptr;
        operatorStateManagedFuture = nullptr;
        operatorStateRawFuture = nullptr;
        inputChannelStateFuture = nullptr;
        resultSubpartitionStateFuture = nullptr;

        // Initialize input channel state future with completed future
        //        auto inputPromise =
        //                std::make_shared<std::promise<SnapshotResult<StateObjectCollection<InputChannelStateHandle>>>>();
        //        inputPromise->set_value(SnapshotResult<StateObjectCollection<InputChannelStateHandle>>(nullptr,
        //        nullptr)); inputChannelStateFuture = std::make_shared<std::future<SnapshotResult
        //                <StateObjectCollection<InputChannelStateHandle>>>>(inputPromise->get_future());
        //
        //        // Initialize result subpartition state future with completed future
        //        auto resultPromise = std::make_shared<std::promise<SnapshotResult
        //                <StateObjectCollection<ResultSubpartitionStateHandle>>>>();
        //        resultPromise->set_value(
        //            SnapshotResult<StateObjectCollection<ResultSubpartitionStateHandle>>(nullptr, nullptr));
        //        resultSubpartitionStateFuture = std::make_shared<std::packaged_task<SnapshotResult<
        //                StateObjectCollection<ResultSubpartitionStateHandle>>>>(resultPromise->get_future());
    };

    OperatorSnapshotFutures(
        std::shared_ptr<std::packaged_task<std::shared_ptr<SnapshotResult<KeyedStateHandle>>()>>
            keyedStateManagedFuture,
        std::shared_ptr<std::packaged_task<std::shared_ptr<SnapshotResult<KeyedStateHandle>>()>> keyedStateRawFuture,
        std::shared_ptr<std::packaged_task<std::shared_ptr<SnapshotResult<OperatorStateHandle>>()>>
            operatorStateManagedFuture,
        std::shared_ptr<std::packaged_task<std::shared_ptr<SnapshotResult<OperatorStateHandle>>()>>
            operatorStateRawFuture,
        std::shared_ptr<
            std::packaged_task<std::shared_ptr<SnapshotResult<StateObjectCollection<InputChannelStateHandle>>>()>>
            inputChannelStateFuture,
        std::shared_ptr<
            std::packaged_task<std::shared_ptr<SnapshotResult<StateObjectCollection<ResultSubpartitionStateHandle>>>()>>
            resultSubpartitionStateFuture)
        : keyedStateManagedFuture(keyedStateManagedFuture),
          keyedStateRawFuture(keyedStateRawFuture),
          operatorStateManagedFuture(operatorStateManagedFuture),
          operatorStateRawFuture(operatorStateRawFuture),
          inputChannelStateFuture(inputChannelStateFuture),
          resultSubpartitionStateFuture(resultSubpartitionStateFuture)
    {
    }

    std::shared_ptr<std::packaged_task<std::shared_ptr<SnapshotResult<KeyedStateHandle>>()>>
    getKeyedStateManagedFuture() const
    {
        return keyedStateManagedFuture;
    }

    void setKeyedStateManagedFuture(
        std::shared_ptr<std::packaged_task<std::shared_ptr<SnapshotResult<KeyedStateHandle>>()>> future)
    {
        keyedStateManagedFuture = future;
    }

    std::shared_ptr<std::packaged_task<std::shared_ptr<SnapshotResult<KeyedStateHandle>>()>> getKeyedStateRawFuture()
        const
    {
        return keyedStateRawFuture;
    }

    void setKeyedStateRawFuture(
        std::shared_ptr<std::packaged_task<std::shared_ptr<SnapshotResult<KeyedStateHandle>>()>> future)
    {
        keyedStateRawFuture = future;
    }

    std::shared_ptr<std::packaged_task<std::shared_ptr<SnapshotResult<OperatorStateHandle>>()>>
    getOperatorStateManagedFuture() const
    {
        return operatorStateManagedFuture;
    }

    void setOperatorStateManagedFuture(
        std::shared_ptr<std::packaged_task<std::shared_ptr<SnapshotResult<OperatorStateHandle>>()>> future)
    {
        operatorStateManagedFuture = future;
    }

    std::shared_ptr<std::packaged_task<std::shared_ptr<SnapshotResult<OperatorStateHandle>>()>>
    getOperatorStateRawFuture() const
    {
        return operatorStateRawFuture;
    }

    void setOperatorStateRawFuture(
        std::shared_ptr<std::packaged_task<std::shared_ptr<SnapshotResult<OperatorStateHandle>>()>> future)
    {
        operatorStateRawFuture = future;
    }

    std::shared_ptr<
        std::packaged_task<std::shared_ptr<SnapshotResult<StateObjectCollection<InputChannelStateHandle>>>()>>
    getInputChannelStateFuture() const
    {
        return inputChannelStateFuture;
    }

    void setInputChannelStateFuture(
        std::shared_ptr<
            std::packaged_task<std::shared_ptr<SnapshotResult<StateObjectCollection<InputChannelStateHandle>>>()>>
            future)
    {
        inputChannelStateFuture = future;
    }

    std::shared_ptr<
        std::packaged_task<std::shared_ptr<SnapshotResult<StateObjectCollection<ResultSubpartitionStateHandle>>>()>>
    getResultSubpartitionStateFuture() const
    {
        return resultSubpartitionStateFuture;
    }

    void setResultSubpartitionStateFuture(
        std::shared_ptr<
            std::packaged_task<std::shared_ptr<SnapshotResult<StateObjectCollection<ResultSubpartitionStateHandle>>>()>>
            future)
    {
        resultSubpartitionStateFuture = future;
    }

    std::pair<long, long> cancel(const std::string& reason = "checkpoint declined due to operator error")
    {
        // 取消所有已设置的 futures，避免已有有效 future 继续执行并报告成功，
        // 导致 TM 侧报错但 JM 看到 SP 任务成功的问题。
        int resetCount = 0;
        if (keyedStateManagedFuture != nullptr) {
            keyedStateManagedFuture.reset();
            resetCount++;
        }
        if (keyedStateRawFuture != nullptr) {
            keyedStateRawFuture.reset();
            resetCount++;
        }
        if (operatorStateManagedFuture != nullptr) {
            operatorStateManagedFuture.reset();
            resetCount++;
        }
        if (operatorStateRawFuture != nullptr) {
            operatorStateRawFuture.reset();
            resetCount++;
        }
        if (inputChannelStateFuture != nullptr) {
            inputChannelStateFuture.reset();
            resetCount++;
        }
        if (resultSubpartitionStateFuture != nullptr) {
            resultSubpartitionStateFuture.reset();
            resetCount++;
        }

        INFO_RELEASE(
            "OperatorSnapshotFutures::cancel reason=[" << reason << "], resetCount=" << resetCount
                                                       << ", injecting failed keyedStateManagedFuture");

        // 注入失败状态的 keyedStateManagedFuture，将真实失败原因传递到 JM
        auto failedPromise = std::make_shared<std::promise<std::shared_ptr<SnapshotResult<KeyedStateHandle>>>>();
        failedPromise->set_exception(std::make_exception_ptr(std::runtime_error(reason)));
        keyedStateManagedFuture =
            std::make_shared<std::packaged_task<std::shared_ptr<SnapshotResult<KeyedStateHandle>>()>>(
                [promise = std::move(failedPromise)]() { return promise->get_future().get(); });

        return std::make_pair(0, 0);
    }
    void OperatorSemInit()
    {
        std::lock_guard<std::mutex> lock(waitMutex);
        ++waitcount;
    }
    void OperatorSemPost()
    {
        bool notify = false;
        {
            std::lock_guard<std::mutex> lock(waitMutex);
            if (waitcount == 0) {
                INFO_RELEASE("Exception: Operator snapshot wait post without pending callback.");
                return;
            }
            --waitcount;
            notify = waitcount == 0;
        }
        if (notify) {
            waitCv.notify_all();
        }
    }

    void OperatorSemWait()
    {
        std::unique_lock<std::mutex> lock(waitMutex);
        waitCv.wait(lock, [this]() { return waitcount == 0; });
    }

private:
    std::shared_ptr<std::packaged_task<std::shared_ptr<SnapshotResult<KeyedStateHandle>>()>> keyedStateManagedFuture;
    std::shared_ptr<std::packaged_task<std::shared_ptr<SnapshotResult<KeyedStateHandle>>()>> keyedStateRawFuture;
    std::shared_ptr<std::packaged_task<std::shared_ptr<SnapshotResult<OperatorStateHandle>>()>>
        operatorStateManagedFuture;
    std::shared_ptr<std::packaged_task<std::shared_ptr<SnapshotResult<OperatorStateHandle>>()>> operatorStateRawFuture;
    std::shared_ptr<
        std::packaged_task<std::shared_ptr<SnapshotResult<StateObjectCollection<InputChannelStateHandle>>>()>>
        inputChannelStateFuture;
    std::shared_ptr<
        std::packaged_task<std::shared_ptr<SnapshotResult<StateObjectCollection<ResultSubpartitionStateHandle>>>()>>
        resultSubpartitionStateFuture;
    std::mutex waitMutex;
    std::condition_variable waitCv;
    int waitcount = 0;
};

#endif // OMNISTREAM_OPERATORSNAPSHOTFUTURES
