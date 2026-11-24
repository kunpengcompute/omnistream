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

#ifndef FLINK_TNEL_INPUTSTATUS_H
#define FLINK_TNEL_INPUTSTATUS_H

// 定义 InputStatus 枚举类
enum class InputStatus {
    // 指示有更多数据可用，并且可以立即再次调用输入以产生更多数据
    MORE_AVAILABLE,
    // 指示当前没有数据可用，但未来还会有更多数据可用
    NOTHING_AVAILABLE,
    // 指示输入已经到达数据的末尾
    END_OF_INPUT
};

#endif
