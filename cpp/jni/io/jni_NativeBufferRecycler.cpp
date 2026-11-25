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

#include <taskmanager/OmniTask.h>
#include <nlohmann/json.hpp>
#include <string>
#include "common.h"
#include "com_huawei_omniruntime_flink_runtime_io_network_buffer_NativeBufferRecycler.h"
#include "runtime/io/network/netty/OmniCreditBasedSequenceNumberingViewReader.h"
#include "runtime/partition/consumer/OmniLocalChannelReader.h"
#include "com_huawei_omniruntime_flink_runtime_io_network_buffer_NativeNetworkBufferRecycler.h"

JNIEXPORT void JNICALL
Java_com_huawei_omniruntime_flink_runtime_io_network_buffer_NativeBufferRecycler_freeNativeByteBuffer(
    JNIEnv* jniEnv, jobject input, jlong nativeViewReaderRef, jlong nativeByteBufferAddressRef)
{
    auto nativeViewReader = reinterpret_cast<omnistream::BufferAvailabilityListener*>(nativeViewReaderRef);
    if (auto creditReader = dynamic_cast<omnistream::OmniCreditBasedSequenceNumberingViewReader*>(nativeViewReader)) {
        creditReader->RecycleNettyBuffer(nativeByteBufferAddressRef);
    } else if (auto localChannelReader = dynamic_cast<omnistream::OmniLocalChannelReader*>(nativeViewReader)) {
        localChannelReader->recycleMemorySegment(nativeByteBufferAddressRef);
    }
}

JNIEXPORT void JNICALL
Java_com_huawei_omniruntime_flink_runtime_io_network_buffer_NativeNetworkBufferRecycler_freeNativeByteBuffer(
    JNIEnv* jniEnv, jobject input, jlong nativeViewReaderRef, jlong nativeByteBufferAddressRef)
{
    auto nativeViewReader =
        reinterpret_cast<omnistream::OmniCreditBasedSequenceNumberingViewReader*>(nativeViewReaderRef);
    if (nativeViewReader != nullptr) {
        nativeViewReader->RecycleNetworkBuffer(nativeByteBufferAddressRef);
    }
}
