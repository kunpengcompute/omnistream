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

#ifndef OMNISTREAM_OMNILOCALINPUTCHANNELBRIDGEIMPL_H
#define OMNISTREAM_OMNILOCALINPUTCHANNELBRIDGEIMPL_H

#include "jni/common/global.h"
#include "runtime/state/bridge/OmniLocalInputChannelBridge.h"

class OmniLocalInputChannelBridgeImpl : public omnistream::OmniLocalInputChannelBridge {
public:
    ~OmniLocalInputChannelBridgeImpl();

    void RegisterJavaOmniLocalInputChannel(JNIEnv* env, jobject javaRemoteDataFetcher);

    void ClearJavaChannelRef();

    void InvokeDoResumeConsumption() override; // call Java doResumeConsumption()
private:
    jobject javaOmniLocalInputChannel = nullptr;
};
#endif // OMNISTREAM_OMNILOCALINPUTCHANNELBRIDGEIMPL_H
