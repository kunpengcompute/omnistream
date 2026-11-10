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

#include <algorithm>
#include <cstring>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "common.h"
#include "core/utils/ByteView.h"
#include "runtime/state/CheckpointStateOutputStreamProxy.h"
#include "runtime/state/FullSnapshotResources.h"
#include "runtime/state/KeyGroupRangeOffsets.h"
#include "runtime/state/KeyGroupsSavepointStateHandle.h"
#include "runtime/state/KeyGroupsStateHandle.h"
#include "runtime/state/SavepointKvStreamWriter.h"
#include "runtime/state/vbsave/VectorBatchSaveHooks.h"
#include "runtime/state/vbsave/VectorBatchSavePlan.h"
#include "runtime/state/bridge/OmniTaskBridge.h"
#include "runtime/checkpoint/CheckpointOptions.h"

namespace omnistream {

// 兼容 savepoint 保存方向公共调度流程。
// 根据 stateType 透传状态或调用 Adaptor 转换，每条 source entry 可以输出 0-N 条 target entry。
class VectorBatchSaveFlow final {
public:
    template <typename Hooks>
    static void executeSave(
        Hooks& hooks,
        const VectorBatchSavePlan& plan,
        CheckpointStateOutputStreamProxy& stream,
        KeyGroupRangeOffsets& keyGroupOffsets,
        FullSnapshotResources& snapshotResources,
        std::string keySerializer)
    {
        // Step 1: 写入目标 Flink logical metadata
        stream.writeMetadata(plan.targetMetaInfos, std::move(keySerializer));

        // Step 2: 构造保存上下文
        auto stateContexts = hooks.buildSaveStateContexts(snapshotResources, plan);

        // Step 3: 创建状态迭代器
        auto iterator = snapshotResources.createKVStateIterator();
        if (!iterator) {
            INFO_RELEASE("Error:VectorBatchSaveFlow::executeSave failed to create iterator");
            throw std::runtime_error("VectorBatchSaveFlow: failed to create iterator");
        }

        // Step 4: 构造 SP 流写入
        SavepointKvStreamWriter writer(stream, keyGroupOffsets);

        try {
            // Step 5: 根据 kvStateId 获取对应上下文
            auto getContext = [&](int kvStateId) -> const VectorBatchSaveStateContext& {
                if (kvStateId < 0 || static_cast<size_t>(kvStateId) >= stateContexts.size() ||
                    !stateContexts[kvStateId].isValid()) {
                    INFO_RELEASE(
                        "Error:VectorBatchSaveFlow::executeSave invalid source kvStateId="
                        << kvStateId << ", stateContextCount=" << stateContexts.size());
                    throw std::runtime_error(
                        "VectorBatchSaveFlow: invalid source kvStateId " + std::to_string(kvStateId));
                }
                return stateContexts[kvStateId];
            };

            // Step 6: 定义统一的 target entry 写出逻辑。 首条 target entry 使用 writeFirst，同一 source entry
            // 的后续结果不重复写边界信息。
            bool hasOutput = false;
            bool pendingNewKeyGroup = false;
            bool pendingNewKeyValueState = false;
            auto emitEntry = [&](const ConvertedEntry& converted,
                                 const KeyValueStateIterator::CurrentEntry& sourceEntry,
                                 bool& sourceHasOutput) {
                if (converted.context == nullptr) {
                    throw std::runtime_error("VectorBatchSaveFlow: converted entry has null context");
                }
                if (converted.context->mappedKvStateId < 0 ||
                    static_cast<size_t>(converted.context->mappedKvStateId) >= plan.targetMetaInfos.size()) {
                    throw std::runtime_error("VectorBatchSaveFlow: mapped kvStateId out of target metadata range");
                }
                ByteView key(reinterpret_cast<const int8_t*>(converted.keyBytes.data()), converted.keyBytes.size());
                ByteView value(
                    reinterpret_cast<const int8_t*>(converted.valueBytes.data()), converted.valueBytes.size());
                if (!hasOutput) {
                    writer.writeFirst(key, value, converted.context->mappedKvStateId, sourceEntry.keyGroup);
                    hasOutput = true;
                } else {
                    writer.writeNext(
                        key,
                        value,
                        converted.context->mappedKvStateId,
                        sourceEntry.keyGroup,
                        !sourceHasOutput && pendingNewKeyGroup,
                        !sourceHasOutput && pendingNewKeyValueState);
                }
                sourceHasOutput = true;
                pendingNewKeyGroup = false;
                pendingNewKeyValueState = false;
            };

            // Step 7: 遍历，普通 KV/PQ 直接透传；其余状态由 Adaptor 转换并输出 0-N 条结果。
            auto convertEntry = [&](const KeyValueStateIterator::CurrentEntry& entry, auto&& output) {
                const auto& ctx = getContext(entry.kvStateId);
                if (ctx.stateType == VectorBatchStateType::KV || ctx.stateType == VectorBatchStateType::PQ) {
                    ConvertedEntry converted;
                    converted.context = &ctx;
                    converted.keyBytes.resize(entry.key.size());
                    std::memcpy(converted.keyBytes.data(), entry.key.data(), entry.key.size());
                    converted.valueBytes.resize(entry.value.size());
                    std::memcpy(converted.valueBytes.data(), entry.value.data(), entry.value.size());
                    output(std::move(converted));
                    return;
                }
                hooks.convertKVRowData(entry, ctx, plan, std::forward<decltype(output)>(output));
            };

            while (iterator->isValid()) {
                const auto& entry = iterator->current();
                pendingNewKeyGroup = pendingNewKeyGroup || entry.newKeyGroup;
                pendingNewKeyValueState = pendingNewKeyValueState || entry.newKeyValueState;
                if (std::find(plan.mainStateIds.begin(), plan.mainStateIds.end(), entry.kvStateId) ==
                    plan.mainStateIds.end()) {
                    iterator->next();
                    continue;
                }
                bool sourceHasOutput = false;
                convertEntry(entry, [&](ConvertedEntry converted) { emitEntry(converted, entry, sourceHasOutput); });
                iterator->next();
            }

            // Step 8: 完成 writer 写出并关闭迭代器。
            writer.finish();
            iterator->close();
        } catch (const std::exception& e) {
            iterator->close();
            INFO_RELEASE("Error:VectorBatchSaveFlow::executeSave failed, reason=" << e.what());
            throw;
        }
    }

private:
    VectorBatchSaveFlow() = delete;
};

} // namespace omnistream
