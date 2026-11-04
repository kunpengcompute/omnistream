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

#ifndef OMNISTREAM_VALUES_SOURCE_FUNCTION_H
#define OMNISTREAM_VALUES_SOURCE_FUNCTION_H

#include <atomic>
#include <cstdint>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

#include "functions/AbstractRichFunction.h"
#include "functions/SourceFunction.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "types/logical/LogicalType.h"

namespace omnistream {

/**
 * Native VALUES source.
 *
 * The Java adapter (OmniAdaptor/omni-table-planner CommonExecValues) injects a JSON
 * description into the transformation: {format:"values", tuples:[[...]], outputTypes:[...],
 * outputNames:[...], batch:true}. This source parses the description, converts the literal
 * tuples into a bounded VectorBatch and emits it through StreamSource.
 *
 * The source is stateless (like Flink's ValuesInputFormat source) and runs with parallelism 1.
 * It is marked as Flink-savepoint compatible (no managed state).
 */
class ValuesSourceFunction : public SourceFunction<omnistream::VectorBatch>, public AbstractRichFunction {
public:
    explicit ValuesSourceFunction(const nlohmann::json& config);

    void run(SourceContext* ctx) override;

    void cancel() override;

    void close() override;

    /**
     * Convert literal tuples (2D JSON array) into a VectorBatch.
     *
     * @param tuples    2D JSON array, each cell is a string value or null.
     * @param fieldTypes per-column native DataTypeId.
     * @param precisions per-column decimal/timestamp precision (0 when not applicable).
     * @param scales     per-column decimal scale (0 when not applicable).
     * @return a newly allocated VectorBatch, caller takes ownership.
     */
    static omnistream::VectorBatch* convert(
        const nlohmann::json& tuples,
        const std::vector<omniruntime::type::DataTypeId>& fieldTypes,
        const std::vector<int32_t>& precisions,
        const std::vector<int32_t>& scales);

private:
    bool reachedEnd() const;

    omnistream::VectorBatch* nextRecord();

    static bool IsSupportedType(omniruntime::type::DataTypeId typeId);

    nlohmann::json tuples_;
    std::vector<omniruntime::type::DataTypeId> fieldTypes_;
    std::vector<int32_t> precisions_;
    std::vector<int32_t> scales_;
    std::atomic<bool> isRunning_;
    bool emitted_;
};

} // namespace omnistream

#endif // OMNISTREAM_VALUES_SOURCE_FUNCTION_H
