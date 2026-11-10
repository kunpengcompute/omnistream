/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#pragma once

#include <string>

namespace omnistream {

// 保存阶段的状态过滤工具。
// TODO: adaptor 保存侧移除 vb 侧表过滤后，删除此工具类及对应头文件。
class VectorBatchSaveTools {
public:
    // TODO: 状态后端停止注册 vb 侧表后，删除保存侧的侧表过滤及此方法。
    // 以 "vb" 结尾的遗留侧表不写入兼容 savepoint。
    static bool isVbStateName(const std::string& stateName)
    {
        return stateName.size() >= 2 && stateName.substr(stateName.size() - 2) == "vb";
    }
};

} // namespace omnistream
