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

#ifndef OMNISTREAM_REMOTEDATAFETCHERBRIDGE_H
#define OMNISTREAM_REMOTEDATAFETCHERBRIDGE_H

#include "SingleInputGate.h"
#include <vector>
#include <memory>

namespace omnistream {
class RemoteDataFetcherBridge : public std::enable_shared_from_this<RemoteDataFetcherBridge> {
public:
    // Implementations own a JNI global reference, so this base needs a virtual destructor for it to
    // run through a base-class pointer.
    virtual ~RemoteDataFetcherBridge() = default;

    virtual void InvokeJavaRemoteDataFetcherResumeConsumption(int inputGateIndex, int channelIndex) = 0;

    // Drops the JNI global reference on the Java RemoteDataFetcher.
    //
    // InitCppRemoteInputChannel hands every remote input channel a shared_ptr to this bridge, and
    // those channels outlive the task -- they are released only once drained. The destructor
    // therefore does not run at task teardown, so the reference has to be released explicitly or
    // the Java RemoteDataFetcher stays alive, and with it the input gate and the whole OmniTask.
    virtual void ReleaseJavaRemoteDataFetcher()
    {
    }

    virtual void InitCppRemoteInputChannel(std::vector<std::shared_ptr<SingleInputGate>> inputGates);
};
} // namespace omnistream
#endif // OMNISTREAM_REMOTEDATAFETCHERBRIDGE
