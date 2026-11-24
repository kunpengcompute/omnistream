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

#include <jni.h>
#include <exception>
#include "runtime/state/NativeRocksDBResourceRegistry.h"

extern "C" JNIEXPORT jlong JNICALL
Java_com_huawei_omniruntime_flink_runtime_state_OmniRocksDBNativeBridge_createSharedResources(
    JNIEnv* env, jclass, jlong bytes, jdouble writeRatio, jdouble priorityRatio, jboolean partitioned)
{
    try {
        return NativeRocksDBResourceRegistry::instance().create(bytes, writeRatio, priorityRatio, partitioned);
    } catch (const std::exception& e) {
        env->ThrowNew(env->FindClass("java/lang/IllegalArgumentException"), e.what());
    } catch (...) {
        env->ThrowNew(env->FindClass("java/lang/IllegalStateException"), "Unable to create Omni RocksDB resources");
    }
    return 0;
}

extern "C" JNIEXPORT void JNICALL
Java_com_huawei_omniruntime_flink_runtime_state_OmniRocksDBNativeBridge_releaseSharedResources(
    JNIEnv* env, jclass, jlong handle)
{
    try {
        NativeRocksDBResourceRegistry::instance().release(handle);
    } catch (const std::exception& e) {
        env->ThrowNew(env->FindClass("java/lang/IllegalStateException"), e.what());
    } catch (...) {
        env->ThrowNew(env->FindClass("java/lang/IllegalStateException"), "Unable to release Omni RocksDB resources");
    }
}
