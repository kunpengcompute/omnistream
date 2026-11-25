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

#ifndef OMNISTREAM_REMOTEDATAFETCHERBRIDGEIMPL_H
#define OMNISTREAM_REMOTEDATAFETCHERBRIDGEIMPL_H
#include <common/global.h>

#include "runtime/partition/consumer/RemoteDataFetcherBridge.h"

class RemoteDataFetcherBridgeImpl : public omnistream::RemoteDataFetcherBridge {
public:
    RemoteDataFetcherBridgeImpl() = default;

    ~RemoteDataFetcherBridgeImpl();

    void SetJavaRemoteDataFetcher(JNIEnv* env, jobject fetcher);

    void InvokeJavaRemoteDataFetcherResumeConsumption(int inputGateIndex, int channelIndex) override;

    void CleanJavaRemoteDataFetcher();

    void ReleaseJavaRemoteDataFetcher() override;

private:
    jobject javaRemoteDataFetcherRef_ = nullptr;
};
#endif // OMNISTREAM_REMOTEDATAFETCHERBRIDGEIMPL_H
