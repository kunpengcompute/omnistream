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

#include <iostream>
#include "global.h"

JavaVM* g_OmniStreamJVM = nullptr;

// JNI_OnLoad is called when the native library is loaded by System.loadLibrary()
JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM* vm, void* reserved)
{
    g_OmniStreamJVM = vm; // Save the JavaVM pointer
    JNIEnv* env;
    if (vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_8) != JNI_OK) {
        return JNI_ERR; // JNI_VERSION_1_8 is recommended
    }
    std::cout << "JNI_OnLoad called. JavaVM pointer saved." << std::endl;
    return JNI_VERSION_1_8;
}

// JNI_OnUnload is called when the class loader containing the native library is garbage collected
JNIEXPORT void JNICALL JNI_OnUnload(JavaVM* vm, void* reserved)
{
    g_OmniStreamJVM = nullptr; // Clear the JavaVM pointer
    std::cout << "JNI_OnUnload called. JavaVM pointer cleared." << std::endl;
}
