/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * omnistream is licensed under Mulan PSL v2.
 * You can use this
 * software according to the terms and conditions of the Mulan
 * PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:

 * *         http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES
 * OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT
 * FOR A PARTICULAR PURPOSE.
 */

#pragma once

#include <memory>
#include <type_traits>
#include "runtime/state/restore/vb/VectorBatchRestoreHooks.h"
#include <unordered_map>
#include <vector>

#include "common.h"
#include "runtime/state/metainfo/StateMetaInfoSnapshot.h"
#include "runtime/state/restore/RestoreBackendDelegate.h"
#include "runtime/state/restore/SavepointRestoreResultIterator.h"

namespace omnistream {

// 恢复方向公共调度流程：遍历 Flink logical source entries，通过 Adaptor 提供的
// 按 StateType 分发到 KV / KV_TRANSFORM / KV_TRANSFORMED / PQ 子流程。
// Derived 需实现:  getStateType / buildOmniMainMetaInfo / transformKVData

class VectorBatchRestoreFlow final {
public:
    template <typename Derived>
    static void executeRestore(
        Derived& derived, SavepointRestoreResultIterator& restoreIterator, RestoreBackendDelegate& backend)
    {
        LOG("VectorBatchRestoreFlow::executeRestore - start (dispatch model)");

        int restoreResultIndex = 0;
        while (restoreIterator.hasNext()) {
            auto restoreResult = restoreIterator.next();
            auto& metaInfos = restoreResult->getStateMetaInfoSnapshots();
            LOG("VectorBatchRestoreFlow: restoreResult#" << restoreResultIndex++
                                                         << ", metaInfoCount=" << metaInfos.size());

            // Step 1: 预创建所有 writer（按 kvStateId → type 分类）
            std::unordered_map<int, std::unique_ptr<RestoreKVState>> kvWriters;
            std::unordered_map<int, std::unique_ptr<RestorePQState>> pqWriters;
            std::unordered_map<int, RestoreStateType> stateTypeMap;

            for (int i = 0; i < metaInfos.size(); ++i) {
                RestoreStateType stateType = derived.getStateType(metaInfos[i]);
                stateTypeMap[i] = stateType;

                switch (stateType) {
                    case RestoreStateType::KV: {
                        kvWriters[i] = backend.createKVState(i, metaInfos[i]);
                        break;
                    }
                    case RestoreStateType::KV_TRANSFORM:
                    case RestoreStateType::KV_TRANSFORMED: {
                        auto mainMetaInfo = derived.buildOmniMainMetaInfo(i, metaInfos[i]);
                        kvWriters[i] = backend.createKVState(i, mainMetaInfo);
                        break;
                    }
                    case RestoreStateType::PQ: {
                        pqWriters[i] = backend.createPQState(i, metaInfos[i]);
                        break;
                    }
                    default: INFO_RELEASE("VectorBatchRestoreFlow::executeRestore get UNSUPPORT state type."); break;
                }
            }

            // Step 2: 单次遍历 keyGroup→entry，按 kvStateId 路由到对应 writer
            auto keyGroupIterator = restoreResult->getKeyGroupIterator();
            try {
                while (keyGroupIterator->hasNext()) {
                    auto keyGroup = keyGroupIterator->next();
                    int keyGroupId = keyGroup->getKeyGroupId();
                    auto entryIter = keyGroup->getKeyGroupEntries();

                    while (entryIter->hasNext()) {
                        auto entry = entryIter->next();
                        int kvStateId = entry.getKvStateId();

                        auto stIt = stateTypeMap.find(kvStateId);
                        if (stIt == stateTypeMap.end()) {
                            continue;
                        }

                        try {
                            switch (stIt->second) {
                                case RestoreStateType::KV:
                                case RestoreStateType::KV_TRANSFORMED: {
                                    auto& w = kvWriters[kvStateId];
                                    w->setKeyGroupId(keyGroupId);
                                    ByteView valueView(entry.getValue().data(), entry.getValue().size());
                                    w->writeEntry(entry.getKey(), valueView);
                                    break;
                                }
                                case RestoreStateType::KV_TRANSFORM: {
                                    auto& w = kvWriters[kvStateId];
                                    w->setKeyGroupId(keyGroupId);
                                    derived.transformKVData(entry.getKey(), entry.getValue(), kvStateId, w.get());
                                    break;
                                }
                                case RestoreStateType::PQ: {
                                    auto& w = pqWriters[kvStateId];
                                    w->writeEntry(entry.getKey(), entry.getValue());
                                    break;
                                }
                                default:
                                    INFO_RELEASE(
                                        "VectorBatchRestoreFlow::executeRestore get no writer : UNSUPPORT state type");
                                    break;
                            }
                        } catch (const std::exception& e) {
                            const std::string stateName =
                                kvStateId >= 0 && static_cast<size_t>(kvStateId) < metaInfos.size()
                                    ? metaInfos[static_cast<size_t>(kvStateId)].getName()
                                    : "<unknown>";
                            ERROR_RELEASE(
                                "VectorBatchRestoreFlow: entry restore failed"
                                << ", restoreResultIndex=" << (restoreResultIndex - 1) << ", keyGroupId=" << keyGroupId
                                << ", kvStateId=" << kvStateId << ", stateName=" << stateName << ", stateType="
                                << static_cast<int>(stIt->second) << ", keySize=" << entry.getKey().size()
                                << ", valueSize=" << entry.getValue().size() << ", reason=" << e.what());
                            throw;
                        }
                    }
                }

                // 统一 flush 所有 writer
                for (auto& [id, w] : kvWriters) w->flush();
                for (auto& [id, w] : pqWriters) w->flush();
            } catch (...) {
                for (auto& [id, w] : kvWriters) w->discard();
                for (auto& [id, w] : pqWriters) w->discard();
                throw;
            }
        }
        LOG("VectorBatchRestoreFlow::executeRestore - complete, restoreResults=" << restoreResultIndex);
    }

private:
    VectorBatchRestoreFlow() = delete;
};

} // namespace omnistream
