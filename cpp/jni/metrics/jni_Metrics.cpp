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

#include <jni_md.h>
#include <jni.h>
#include "common.h"
#include "com_huawei_omniruntime_flink_runtime_metrics_utils_OmniMetricHelper.h"
#include "com_huawei_omniruntime_flink_runtime_metrics_OmniDescriptiveStatisticsHistogram.h"
#include "com_huawei_omniruntime_flink_runtime_metrics_OmniSimpleCounter.h"
#include "com_huawei_omniruntime_flink_runtime_metrics_OmniSizeGauge.h"
#include "com_huawei_omniruntime_flink_runtime_metrics_OmniLongSizeGauge.h"
#include "com_huawei_omniruntime_flink_runtime_metrics_OmniTimeGauge.h"
#include "com_huawei_omniruntime_flink_runtime_metrics_groups_OmniTaskLocalNettyBufferMetricGroup.h"
#include "com_huawei_omniruntime_flink_runtime_metrics_groups_OmniOperatorStateMetricGroup.h"
#include "com_huawei_omniruntime_flink_runtime_metrics_groups_VectorBatchBufferPoolMetricGroup.h"
#include "metrics/groups/VectorBatchBufferPoolMetricGroup.h"
#include "runtime/metrics/groups/TaskMetricGroup.h"
#include "runtime/metrics/SimpleCounter.h"
#include "runtime/metrics/SizeGauge.h"
#include "runtime/metrics/LongSizeGauge.h"
#include "runtime/metrics/TimerGauge.h"
#include "runtime/metrics/DescriptiveStatisticsHistogram.h"
#include "runtime/metrics/groups/TaskLocalNettyBufferMetricGroup.h"
#include "taskmanager/OmniTask.h"

#include "com_huawei_omniruntime_flink_runtime_metrics_groups_OmniTaskExecutorGlobalNettyMetricGroup.h"
#include "com_huawei_omniruntime_flink_runtime_metrics_groups_OmniTaskExecutorGlobalVectorBatchMetricGroup.h"
#include "io/network/OmniShuffleEnvironment.h"
#include "metrics/groups/TaskManagerMetricGroup.h"
#include "taskexecutor/OmniTaskExecutor.h"
using namespace omnistream;

// Updated common helper uses raw pointer for TaskMetricGroup
template <typename T>
static void registerMetric(
    AbstractMetricGroup* abstractMetricGroup,
    const std::string& scope,
    const std::string& metricNameStr,
    std::shared_ptr<T> metric)
{
    auto taskMetricGroup = dynamic_cast<TaskMetricGroup*>(abstractMetricGroup);
    if (taskMetricGroup) {
        size_t pos = scope.find('_');
        std::string firstSubstring = (pos == std::string::npos) ? scope : scope.substr(0, pos);
        if (firstSubstring == "OmniTaskIOMetricGroup") {
            taskMetricGroup->AddTaskIOMetric(metricNameStr, metric);
        } else if (firstSubstring == "OmniInternalOperatorIOMetricGroup") {
            // the operatorName is the second part of the scope
            std::string operatorName = scope.substr(pos + 33);
            taskMetricGroup->AddInternalOperatorIOMetric(operatorName, metricNameStr, metric);
        } else {
            throw std::runtime_error("Unknown metric group: " + firstSubstring);
        }
    } else {
        abstractMetricGroup->AddMetric(metricNameStr, metric);
    }
}

JNIEXPORT jlong JNICALL
Java_com_huawei_omniruntime_flink_runtime_metrics_utils_OmniMetricHelper_createNativeSimpleCounter(
    JNIEnv* jniEnv, jclass, jlong metricGroupRef, jstring scope, jstring metricName)
{
    auto abstractMetricGroup = reinterpret_cast<AbstractMetricGroup*>(metricGroupRef);
    // Convert scope parameter
    const char* scopeCStr = jniEnv->GetStringUTFChars(scope, nullptr);
    if (scopeCStr == nullptr) {
        throw std::runtime_error("Failed to convert jstring to std::string for scope");
    }
    std::string scopeStr(scopeCStr);
    jniEnv->ReleaseStringUTFChars(scope, scopeCStr);
    // Convert metricName parameter
    const char* utf8String = jniEnv->GetStringUTFChars(metricName, nullptr);
    if (utf8String == nullptr) {
        throw std::runtime_error("Failed to convert jstring to std::string");
    }
    std::string metricNameStr(utf8String);
    jniEnv->ReleaseStringUTFChars(metricName, utf8String); // VERY IMPORTANT: Release the string
    auto counter = std::make_shared<omnistream::SimpleCounter>();
    // Use common helper to register the counter with scope
    registerMetric(abstractMetricGroup, scopeStr, metricNameStr, counter);
    return reinterpret_cast<long>(counter.get());
}

JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_metrics_utils_OmniMetricHelper_createNativeTimeGauge(
    JNIEnv* jniEnv, jclass, jlong metricGroupRef, jstring scope, jstring metricName)
{
    auto abstractMetricGroup = reinterpret_cast<AbstractMetricGroup*>(metricGroupRef);
    // Convert scope parameter
    const char* scopeCStr = jniEnv->GetStringUTFChars(scope, nullptr);
    if (scopeCStr == nullptr) {
        throw std::runtime_error("Failed to convert jstring to std::string for scope");
    }
    std::string scopeStr(scopeCStr);
    jniEnv->ReleaseStringUTFChars(scope, scopeCStr);
    // Convert metricName parameter
    const char* utf8String = jniEnv->GetStringUTFChars(metricName, nullptr);
    if (utf8String == nullptr) {
        throw std::runtime_error("Failed to convert jstring to std::string");
    }
    std::string metricNameStr(utf8String);
    jniEnv->ReleaseStringUTFChars(metricName, utf8String); // VERY IMPORTANT: Release the string
    auto timerGauge = std::make_shared<omnistream::TimerGauge>();
    // Use common helper to register the timerGauge with scope
    registerMetric(abstractMetricGroup, scopeStr, metricNameStr, timerGauge);
    return reinterpret_cast<long>(timerGauge.get());
}

JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_metrics_utils_OmniMetricHelper_createNativeSizeGauge(
    JNIEnv* jniEnv, jclass, jlong metricGroupRef, jstring scope, jstring metricName)
{
    auto abstractMetricGroup = reinterpret_cast<AbstractMetricGroup*>(metricGroupRef);
    // Convert scope parameter
    const char* scopeCStr = jniEnv->GetStringUTFChars(scope, nullptr);
    if (scopeCStr == nullptr) {
        throw std::runtime_error("Failed to convert jstring to std::string for scope");
    }
    std::string scopeStr(scopeCStr);
    jniEnv->ReleaseStringUTFChars(scope, scopeCStr);
    // Convert metricName parameter
    const char* utf8String = jniEnv->GetStringUTFChars(metricName, nullptr);
    if (utf8String == nullptr) {
        throw std::runtime_error("Failed to convert jstring to std::string");
    }
    std::string metricNameStr(utf8String);
    jniEnv->ReleaseStringUTFChars(metricName, utf8String); // VERY IMPORTANT: Release the string
    auto sizeGauge = std::make_shared<omnistream::SizeGauge>();
    // Use common helper to register the sizeGauge with scope
    registerMetric(abstractMetricGroup, scopeStr, metricNameStr, sizeGauge);
    return reinterpret_cast<long>(sizeGauge.get());
}

// 64-bit counterpart of createNativeSizeGauge for byte-valued metrics (keyed-state
// data sizes) that can exceed INT_MAX. Mirrors the SizeGauge path but creates a LongSizeGauge.
JNIEXPORT jlong JNICALL
Java_com_huawei_omniruntime_flink_runtime_metrics_utils_OmniMetricHelper_createNativeLongSizeGauge(
    JNIEnv* jniEnv, jclass, jlong metricGroupRef, jstring scope, jstring metricName)
{
    auto abstractMetricGroup = reinterpret_cast<AbstractMetricGroup*>(metricGroupRef);
    // Convert scope parameter
    const char* scopeCStr = jniEnv->GetStringUTFChars(scope, nullptr);
    if (scopeCStr == nullptr) {
        throw std::runtime_error("Failed to convert jstring to std::string for scope");
    }
    std::string scopeStr(scopeCStr);
    jniEnv->ReleaseStringUTFChars(scope, scopeCStr);
    // Convert metricName parameter
    const char* utf8String = jniEnv->GetStringUTFChars(metricName, nullptr);
    if (utf8String == nullptr) {
        throw std::runtime_error("Failed to convert jstring to std::string");
    }
    std::string metricNameStr(utf8String);
    jniEnv->ReleaseStringUTFChars(metricName, utf8String); // VERY IMPORTANT: Release the string
    auto longSizeGauge = std::make_shared<omnistream::LongSizeGauge>();
    // Use common helper to register the longSizeGauge with scope
    registerMetric(abstractMetricGroup, scopeStr, metricNameStr, longSizeGauge);
    return reinterpret_cast<long>(longSizeGauge.get());
}

JNIEXPORT jlong JNICALL
Java_com_huawei_omniruntime_flink_runtime_metrics_utils_OmniMetricHelper_createNativeOmniDescriptiveStatisticsHistogram(
    JNIEnv* jniEnv, jclass, jlong metricGroupRef, jint windowSize, jstring scope, jstring metricName)
{
    auto abstractMetricGroup = reinterpret_cast<AbstractMetricGroup*>(metricGroupRef);
    // Convert scope parameter
    const char* scopeCStr = jniEnv->GetStringUTFChars(scope, nullptr);
    if (scopeCStr == nullptr) {
        throw std::runtime_error("Failed to convert jstring to std::string for scope");
    }
    std::string scopeStr(scopeCStr);
    jniEnv->ReleaseStringUTFChars(scope, scopeCStr);
    // Convert metricName parameter
    const char* utf8String = jniEnv->GetStringUTFChars(metricName, nullptr);
    if (utf8String == nullptr) {
        throw std::runtime_error("Failed to convert jstring to std::string");
    }
    std::string metricNameStr(utf8String);
    jniEnv->ReleaseStringUTFChars(metricName, utf8String); // VERY IMPORTANT: Release the string
    auto histogram = std::make_shared<omnistream::DescriptiveStatisticsHistogram>(windowSize);
    // Use common helper to register the histogram with scope
    registerMetric(abstractMetricGroup, scopeStr, metricNameStr, histogram);
    return reinterpret_cast<long>(histogram.get());
}

JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_metrics_OmniSimpleCounter_getNativeCounter(
    JNIEnv*, jobject, jlong nativeCounter)
{
    // Same lifetime hazard as getNativeCounterNew: the address outlives the counter once the
    // owning task is destroyed, so validate before dereferencing.
    auto counter = reinterpret_cast<SimpleCounter*>(nativeCounter);
    if (!SimpleCounter::IsLive(counter)) {
        return 0;
    }
    return counter->GetCount();
}

JNIEXPORT jint JNICALL Java_com_huawei_omniruntime_flink_runtime_metrics_OmniSizeGauge_getNativeSize(
    JNIEnv*, jobject, jlong nativeSizeGauge)
{
    auto sizeGauge = reinterpret_cast<SizeGauge*>(nativeSizeGauge);
    return sizeGauge->GetValue();
}

// 64-bit getter for LongSizeGauge-backed metrics.
JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_metrics_OmniLongSizeGauge_getNativeSize(
    JNIEnv*, jobject, jlong nativeLongSizeGauge)
{
    auto longSizeGauge = reinterpret_cast<LongSizeGauge*>(nativeLongSizeGauge);
    return longSizeGauge->GetValue();
}

JNIEXPORT jlong JNICALL
Java_com_huawei_omniruntime_flink_runtime_metrics_OmniDescriptiveStatisticsHistogram_getNativeCount(
    JNIEnv*, jobject, jlong nativeHistogram)
{
    auto histogram = reinterpret_cast<DescriptiveStatisticsHistogram*>(nativeHistogram);
    return histogram->GetCount();
}

/*
 * Class:     com_huawei_omniruntime_flink_runtime_metrics_OmniDescriptiveStatisticsHistogram
 * Method:    getStatistics
 * Signature: (J)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL
Java_com_huawei_omniruntime_flink_runtime_metrics_OmniDescriptiveStatisticsHistogram_getStatistics(
    JNIEnv* jniEnv, jobject, jlong nativeHistogram)
{
    auto histogram = reinterpret_cast<DescriptiveStatisticsHistogram*>(nativeHistogram);
    auto statistics = histogram->GetStatistics();
    std::string statsString = statistics->ToString();
    return jniEnv->NewStringUTF(statsString.c_str());
}

JNIEXPORT void JNICALL Java_com_huawei_omniruntime_flink_runtime_metrics_OmniTimeGauge_updateNative(
    JNIEnv*, jobject, jlong nativeTimeGauge)
{
    auto timerGauge = reinterpret_cast<TimerGauge*>(nativeTimeGauge);
    timerGauge->Update();
}

/*
 * Class:     com_huawei_omniruntime_flink_runtime_metrics_OmniTimeGauge
 * Method:    getNativeValue
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_metrics_OmniTimeGauge_getNativeValue(
    JNIEnv*, jobject, jlong nativeTimeGauge)
{
    auto timerGauge = reinterpret_cast<TimerGauge*>(nativeTimeGauge);
    return timerGauge->GetValue();
}

/*
 * Class:     com_huawei_omniruntime_flink_runtime_metrics_OmniTimeGauge
 * Method:    getNativeCount
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_metrics_OmniTimeGauge_getNativeCount(
    JNIEnv*, jobject, jlong nativeTimeGauge)
{
    auto timerGauge = reinterpret_cast<TimerGauge*>(nativeTimeGauge);
    return timerGauge->GetCount();
}

/*
 * Class:     com_huawei_omniruntime_flink_runtime_metrics_OmniTimeGauge
 * Method:    getNativeAccumulatedCount
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_metrics_OmniTimeGauge_getNativeAccumulatedCount(
    JNIEnv*, jobject, jlong nativeTimeGauge)
{
    auto timerGauge = reinterpret_cast<TimerGauge*>(nativeTimeGauge);
    return timerGauge->GetAccumulatedCount();
}

/*
 * Class:     com_huawei_omniruntime_flink_runtime_metrics_OmniTimeGauge
 * Method:    getNativeIsMeasuring
 * Signature: (J)Z
 */
JNIEXPORT jboolean JNICALL Java_com_huawei_omniruntime_flink_runtime_metrics_OmniTimeGauge_getNativeIsMeasuring(
    JNIEnv*, jobject, jlong nativeTimeGauge)
{
    auto timerGauge = reinterpret_cast<TimerGauge*>(nativeTimeGauge);
    return timerGauge->IsMeasuring() ? JNI_TRUE : JNI_FALSE;
}

/*
 * Class:     com_huawei_omniruntime_flink_runtime_metrics_OmniTimeGauge
 * Method:    getNativeMaxSingleMeasurement
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_metrics_OmniTimeGauge_getNativeMaxSingleMeasurement(
    JNIEnv*, jobject, jlong nativeTimeGauge)
{
    auto timerGauge = reinterpret_cast<TimerGauge*>(nativeTimeGauge);
    return timerGauge->GetMaxSingleMeasurement();
}

JNIEXPORT jlong JNICALL
Java_com_huawei_omniruntime_flink_runtime_metrics_groups_OmniTaskLocalNettyBufferMetricGroup_addLocalNettyBufferPoolMetricGroup(
    JNIEnv* jniEnv, jclass, jlong parentGroupRef, jlong nativeTaskRef, jstring groupName, jobjectArray scopes)
{
    auto taskMetricGroup = reinterpret_cast<TaskMetricGroup*>(parentGroupRef);
    auto omniTask = reinterpret_cast<OmniTask*>(nativeTaskRef);
    const char* utf8String = jniEnv->GetStringUTFChars(groupName, nullptr);
    if (utf8String == nullptr) {
        throw std::runtime_error("Failed to convert jstring to std::string");
    }
    std::string groupNameStr(utf8String);
    jniEnv->ReleaseStringUTFChars(groupName, utf8String);
    auto localNettyTaskMetricGroup =
        std::make_shared<TaskLocalNettyBufferMetricGroup>(taskMetricGroup, [omniTask](const std::string& metricName) {
            return omniTask->CreateLocalNettyBufferMetricSupplier(metricName);
        });
    taskMetricGroup->addGroup(groupNameStr, localNettyTaskMetricGroup);
    omniTask->SetTaskLocalNettyBufferMetricGroup(localNettyTaskMetricGroup);
    return reinterpret_cast<long>(localNettyTaskMetricGroup.get());
}

JNIEXPORT jlong JNICALL
Java_com_huawei_omniruntime_flink_runtime_metrics_groups_VectorBatchBufferPoolMetricGroup_addVectorBatchBufferPoolMetricGroup(
    JNIEnv* jniEnv, jclass, jlong parentGroupRef, jlong nativeTaskRef, jstring groupName, jobjectArray scopes)
{
    auto taskMetricGroup = reinterpret_cast<TaskMetricGroup*>(parentGroupRef);
    auto omniTask = reinterpret_cast<OmniTask*>(nativeTaskRef);
    const char* utf8String = jniEnv->GetStringUTFChars(groupName, nullptr);
    if (utf8String == nullptr) {
        throw std::runtime_error("Failed to convert jstring to std::string");
    }
    std::string groupNameStr(utf8String);
    jniEnv->ReleaseStringUTFChars(groupName, utf8String);
    auto taskIOMetricGroup = taskMetricGroup->GetTaskIOMetricGroup();
    auto vectorBatchBufferPoolMetricGroup = std::make_shared<VectorBatchBufferPoolMetricGroup>(taskIOMetricGroup);
    taskIOMetricGroup->addGroup(groupNameStr, vectorBatchBufferPoolMetricGroup);

    omniTask->SetVectorBatchBufferPoolMetricGroup(vectorBatchBufferPoolMetricGroup);
    return reinterpret_cast<long>(vectorBatchBufferPoolMetricGroup.get());
}

JNIEXPORT jlong JNICALL
Java_com_huawei_omniruntime_flink_runtime_metrics_groups_OmniTaskExecutorGlobalNettyMetricGroup_addTaskExecutorGlobalNettyMetricGroup(
    JNIEnv* jniEnv, jclass, jlong parentGroupRef, jlong nativeTaskExecutorRef, jstring groupName, jobjectArray scopes)
{
    auto taskManagerMetricGroup = reinterpret_cast<TaskManagerMetricGroup*>(parentGroupRef);
    auto omniTaskExecutor = reinterpret_cast<OmniTaskExecutor*>(nativeTaskExecutorRef);
    const char* utf8String = jniEnv->GetStringUTFChars(groupName, nullptr);
    if (utf8String == nullptr) {
        throw std::runtime_error("Failed to convert jstring to std::string");
    }
    std::string groupNameStr(utf8String);
    jniEnv->ReleaseStringUTFChars(groupName, utf8String);
    auto shuffleEnvironment = omniTaskExecutor->GetTaskManagerService()->getShuffleEnvironment();
    OmniShuffleEnvironment* omniShuffleEnvironment = static_cast<OmniShuffleEnvironment*>(shuffleEnvironment.get());
    auto globalNettyPool = omniShuffleEnvironment->getGlobalNettyBufferPool();
    std::shared_ptr<GlobalNettyBufferMetricGroup> globalNettyBufferMetricGroup =
        std::make_shared<GlobalNettyBufferMetricGroup>(
            taskManagerMetricGroup, globalNettyPool->CreateGlobalNettyBufferMetricSupplierFactory());
    taskManagerMetricGroup->addGroup(groupNameStr, globalNettyBufferMetricGroup);
    return reinterpret_cast<long>(globalNettyBufferMetricGroup.get());
}

JNIEXPORT jlong JNICALL
Java_com_huawei_omniruntime_flink_runtime_metrics_groups_OmniTaskExecutorGlobalVectorBatchMetricGroup_addTaskExecutorGlobalVectorBatchMetricGroup(
    JNIEnv* jniEnv, jclass, jlong parentGroupRef, jlong nativeTaskExecutorRef, jstring groupName, jobjectArray scopes)
{
    auto taskManagerMetricGroup = reinterpret_cast<TaskManagerMetricGroup*>(parentGroupRef);
    auto omniTaskExecutor = reinterpret_cast<OmniTaskExecutor*>(nativeTaskExecutorRef);
    const char* utf8String = jniEnv->GetStringUTFChars(groupName, nullptr);
    if (utf8String == nullptr) {
        throw std::runtime_error("Failed to convert jstring to std::string");
    }
    std::string groupNameStr(utf8String);
    jniEnv->ReleaseStringUTFChars(groupName, utf8String);
    auto shuffleEnvironment = omniTaskExecutor->GetTaskManagerService()->getShuffleEnvironment();
    OmniShuffleEnvironment* omniShuffleEnvironment = static_cast<OmniShuffleEnvironment*>(shuffleEnvironment.get());
    auto globalVectorBatchPool = omniShuffleEnvironment->getNetworkBufferPool();
    std::shared_ptr<GlobalVectorBatchBufferMetricGroup> globalVectorBatchBufferMetricGroup =
        std::make_shared<GlobalVectorBatchBufferMetricGroup>(
            taskManagerMetricGroup, globalVectorBatchPool->CreateGlobalVectorBatchBufferMetricSupplierFactory());
    taskManagerMetricGroup->addGroup(groupNameStr, globalVectorBatchBufferMetricGroup);
    return reinterpret_cast<long>(globalVectorBatchBufferMetricGroup.get());
}

JNIEXPORT jlong JNICALL
Java_com_huawei_omniruntime_flink_runtime_metrics_groups_OmniOperatorStateMetricGroup_addOperatorStateMetricGroup(
    JNIEnv* jniEnv, jclass, jlong parentGroupRef, jstring operatorName)
{
    auto taskMetricGroup = reinterpret_cast<TaskMetricGroup*>(parentGroupRef);
    const char* utf8String = jniEnv->GetStringUTFChars(operatorName, nullptr);
    if (utf8String == nullptr) {
        throw std::runtime_error("Failed to convert jstring to std::string");
    }
    std::string operatorNameStr(utf8String);
    jniEnv->ReleaseStringUTFChars(operatorName, utf8String);
    // GetTaskBackendStateMetricGroup lazily creates and self-registers the group under TaskMetricGroup.
    auto operatorGroup = taskMetricGroup->GetTaskBackendStateMetricGroup()->GetOrCreateOperatorGroup(operatorNameStr);
    return reinterpret_cast<long>(operatorGroup.get());
}
