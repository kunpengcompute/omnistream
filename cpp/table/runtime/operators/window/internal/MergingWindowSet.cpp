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

#include "MergingWindowSet.h"

#include "data/Row.h"
#include "runtime/operators/window/TimeWindow.h"

template <typename K, typename W>
void MergingWindowSet<K, W>::initializeCache(const K& key)
{
    auto cache = cachedSortedWindows.get(key);
    if (!cache) {
        sortedWindows = new std::set<W>;
        auto iter = mapping->iteratorV2();
        while (iter->hasNext()) {
            auto& entry = iter->next();
            if (entry.getKey().has_value()) {
                sortedWindows->emplace(entry.getKey().value());
            } else {
                THROW_RUNTIME_ERROR("Iterator return an entry with empty key.");
            }
        }
        cachedSortedWindows.put(key, sortedWindows);
    } else {
        sortedWindows = cache.value();
    }
}

template <typename K, typename W>
W MergingWindowSet<K, W>::getStateWindow(const W& window)
{
    const auto& optionalRes = mapping->get(window);
    if (optionalRes.has_value()) {
        return optionalRes.value();
    }
    THROW_RUNTIME_ERROR(
        "Cannot find the state of window, startTime:" + std::to_string(window.getStart()) +
        ", endTime: " + std::to_string(window.getEnd()));
}

template <typename K, typename W>
void MergingWindowSet<K, W>::retireWindow(const W& window)
{
    mapping->remove(window);
    auto it = sortedWindows->find(window);
    if (it == sortedWindows->end()) {
        THROW_RUNTIME_ERROR("Window is not in in-flight window set.");
    }
    sortedWindows->erase(it);
}

template <typename K, typename W>
W MergingWindowSet<K, W>::addWindow(const W& newWindow, const MergeFunction& mergeFunction)
{
    reuseCollector_.clear();
    windowAssigner->mergeWindows(newWindow, sortedWindows, reuseCollector_);

    W resultWindow = newWindow;
    bool isNewWindowMerged = false;

    for (const auto& c : reuseCollector_) {
        W mergeResult = c.first;
        auto* mergedWindows = c.second;

        if (mergedWindows->erase(newWindow)) {
            isNewWindowMerged = true;
            resultWindow = mergeResult;
        }

        if (mergedWindows->empty()) {
            continue;
        }
        W mergedStateNamespace = getStateWindow(*mergedWindows->begin());

        std::vector<W> mergedStateWindows;
        for (const auto& mergedWindow : *mergedWindows) {
            const std::optional<W>& optionalRes = mapping->get(mergedWindow);
            if (optionalRes.has_value()) {
                mapping->remove(mergedWindow);
                sortedWindows->erase(mergedWindow);
                if (!(optionalRes.value() == mergedStateNamespace)) {
                    mergedStateWindows.push_back(optionalRes.value());
                }
            }
        }

        mapping->put(mergeResult, mergedStateNamespace);
        sortedWindows->insert(mergeResult);

        if (!(mergedWindows->find(mergeResult) != mergedWindows->end() && mergedWindows->size() == 1)) {
            mergeFunction(mergeResult, *mergedWindows, mergedStateNamespace, mergedStateWindows);
        }
    }

    // the new window created a new, self-contained window without merging
    if (reuseCollector_.empty() || (resultWindow == newWindow && !isNewWindowMerged)) {
        mapping->put(resultWindow, resultWindow);
        sortedWindows->insert(resultWindow);
    }
    return resultWindow;
}

template class MergingWindowSet<std::shared_ptr<RowData>, TimeWindow>;
