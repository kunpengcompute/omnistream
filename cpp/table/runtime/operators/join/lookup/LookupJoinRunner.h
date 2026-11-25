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

#ifndef FLINK_TNEL_LOOKUPJOINRUNNER_H
#define FLINK_TNEL_LOOKUPJOINRUNNER_H

#include <nlohmann/json.hpp>
#include "table/data/vectorbatch/VectorBatch.h"
#include "streaming/api/functions/ProcessFunction.h"
#include "streaming/api/functions/GeneratedCsvLookupFunction.h"
#include "table/runtime/collector/TableFunctionCollector.h"
#include "table/typeutils/InternalTypeInfo.h"
#include "streaming/api/operators/TimestampedCollector.h"
// The original Flink has ProcessFunction as a template class with <IN, OUT>
// here both IN and OUT are VectorBatch*
class LookupJoinRunner : public ProcessFunction<omnistream::VectorBatch*, omnistream::VectorBatch*> {
public:
    explicit LookupJoinRunner(nlohmann::json description, Collector* innerCollector);

    void open(const Configuration& parameters) override;

    void processBatch(omnistream::VectorBatch* value, Context* cxt, Collector* collector) override;

    [[nodiscard]] Collector* getFetcherCollector() const
    {
        return collector;
    }

    void close();

private:
    bool isLeftOuterJoin;
    GeneratedCsvLookupFunction<int64_t>* fetcher;
    TableFunctionCollector* collector;
    Collector* innerCollector; // The actual collector inside TableFunctionCollector.

    // inputTypeInfos is the type of incoming probe side
    nlohmann::json description;
};

#endif
