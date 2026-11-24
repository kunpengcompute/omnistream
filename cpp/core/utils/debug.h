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

#ifndef FLINK_TNEL_DEBUG_H_
#define FLINK_TNEL_DEBUG_H_

#include <chrono>
#include <stdexcept>

using Time = std::chrono::high_resolution_clock;
using ms = std::chrono::milliseconds;
using fsec = std::chrono::duration<float>;

// define time
#if defined(DEBUG_JNI) || defined(DEBUG_OPERATOR) || defined(DEBUG_LLVM)
#define START() Time::now()
#define END(t0)                                    \
    ({                                             \
        auto t1 = Time::now();                     \
        fsec fs = t1 - t0;                         \
        ms d = std::chrono::duration_cast<ms>(fs); \
        d.count();                                 \
    })
#else
#define START() 0
#define END(t0) 0
#endif

#ifdef DEBUG_JNI
#define JNI_DEBUG_LOG(format, ...) printf("[%s][%s][%d]: " format "\n", __FILE__, __FUNCTION__, __LINE__, ##__VA_ARGS__)
#else
#define JNI_DEBUG_LOG(format, ...)
#endif

#ifdef OP_DEBUG
#define OP_DEBUG_LOG(format, ...)                                                                  \
    do {                                                                                           \
        printf("[OPERATOR][%s][%s][%d]:" format, __FILE__, __FUNCTION__, __LINE__, ##__VA_ARGS__); \
    } while (0)
#else
#define OP_DEBUG_LOG(format, ...)
#endif

#ifdef DEBUG_LLVM
#define LLVM_DEBUG_LOG(format, ...)                                                                 \
    do {                                                                                            \
        printf("[LLVM][%s][%s][%d]:" format "\n", __FILE__, __FUNCTION__, __LINE__, ##__VA_ARGS__); \
    } while (0)
#else
#define LLVM_DEBUG_LOG(format, ...)
#endif

#ifdef TRACE
#define LogTrace(format, ...)                                                                        \
    do {                                                                                             \
        printf("[TRACE][%s][%s][%d]:" format "\n", __FILE__, __FUNCTION__, __LINE__, ##__VA_ARGS__); \
    } while (0)
#else
#define LogTrace(format, ...)
#endif

#if defined(DEBUG) || defined(TRACE)
#ifndef ASSERT
#define ASSERT(condition)                                          \
    do {                                                           \
        if (!(condition)) {                                        \
            throw std::runtime_error(#condition " is not match!"); \
        }                                                          \
    } while (0)
#endif
#else
#define ASSERT(condition)
#endif
#endif
