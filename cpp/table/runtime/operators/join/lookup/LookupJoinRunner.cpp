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

#include "LookupJoinRunner.h"

void LookupJoinRunner::processBatch(omnistream::VectorBatch* in, Context* cxt, Collector* out)
{
    // This collector is the TableFunctionCollector in LookupJoinRunner
    // out is the downstream operator
    collector->setCollector(out);
    collector->setInput(in);
    collector->reset();
    fetcher->flatMap(in, collector);

    if (isLeftOuterJoin && !collector->isCollected()) {
        // todo: currently I only implemented InnerJoin
        NOT_IMPL_EXCEPTION;
    }
}

void LookupJoinRunner::open(const Configuration& parameters)
{
    using namespace omniruntime::type;
    std::string temporalTableSourceSpec = description["temporalTableSourceSpec"];
    std::string connectorType = description["connectorType"];
    std::string filepath;
    // todo: check all possitiblity of this parameter! And how to find the file path with it
    if (connectorType == "filesystem") {
        filepath = description["connectorPath"];
    } else {
        NOT_IMPL_EXCEPTION;
    }
    auto* src = new CsvTableSource(filepath, description["lookupInputTypes"].get<std::vector<std::string>>());
    // setup collector
    collector = new TableFunctionCollector();
    collector->setCollector(innerCollector);
    // setup fetcher
    auto lookupFunction = new CsvLookupFunction<int64_t>(description, src);
    fetcher = new GeneratedCsvLookupFunction<int64_t>(lookupFunction);
    fetcher->open();
}

LookupJoinRunner::LookupJoinRunner(nlohmann::json description, Collector* innerCollector)
    : innerCollector(innerCollector),
      description(description)
{
    isLeftOuterJoin = (description["joinType"].get<std::string>() == "LeftOuterJoin");
}

void LookupJoinRunner::close()
{
    delete fetcher;
}
