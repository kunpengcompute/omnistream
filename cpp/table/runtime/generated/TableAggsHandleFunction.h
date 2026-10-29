/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */
#ifndef FLINK_TNEL_TABLE_AGGS_HANDLE_FUNCTION_H
#define FLINK_TNEL_TABLE_AGGS_HANDLE_FUNCTION_H

#include "AggsHandleFunctionBase.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/data/RowData.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "streaming/api/operators/TimestampedCollector.h"

/**
 * 表聚合函数契约。
 *
 * 对齐 Flink 原版：org.apache.flink.table.runtime.generated.TableAggsHandleFunction。
 * 继承关系对齐 Flink（TableAggsHandleFunction extends AggsHandleFunctionBase）；
 * 与单值聚合契约 AggsHandleFunction 平行——单值聚合经 getValue 产出「每组一行」，
 * 表聚合经 emitValue 产出「每组 0..N 行」，并以 isRetract 区分回撤行与新值行。
 *
 * 分工：算子（GroupTableAggregateFunction）只面向本契约编程；具体实现类
 * （如 RowNumberFunction）实现本契约后经函数工厂装配，两侧可独立演进。
 */
class TableAggsHandleFunction : public AggsHandleFunctionBase {
public:
    virtual ~TableAggsHandleFunction() = default;

    // ---- 增量聚合契约（方法语义与 AggsHandleFunction 的单行版本一致）----

    /** 将一行输入累计进累加器（表聚合场景：成员进入榜单）。 */
    virtual void accumulate(RowData* input) = 0;

    /** 从累加器中回退一行输入（表聚合场景：成员移出榜单，响应回撤消息）。 */
    virtual void retract(RowData* input) = 0;

    /** 创建空累加器（写入调用者提供的 accumulators，输出参数风格）。 */
    virtual void createAccumulators(BinaryRowData* accumulators) = 0;

    /** 将状态中的累加器装载进函数（处理每行前的固定动作，与 Flink 源码调用次序一致）。 */
    virtual void setAccumulators(RowData* accumulators) = 0;

    /** 从函数取回累加器（处理后写回状态，输出参数风格）。 */
    virtual void getAccumulators(BinaryRowData* accumulators) = 0;

    /** 清理函数内部资源（分组消亡时与状态清理一同调用）。 */
    virtual void cleanup() = 0;

    /** 释放资源。 */
    virtual void close() = 0;

    /**
     * 占用的 BinaryRowData 累加器槽位数（默认 0：表聚合成员走 keyed MapView 持久层，
     * 与 JsonArrayAggFunction 的 RAW-accType 对齐约定）。
     */
    virtual int accumulatorSlots() const
    {
        return 0;
    }

    /**
     * 向量化批处理入口（默认实现：逐行调用单行版本）。
     * 实现类可按需覆盖为批量优化版本，算子的 processBatch 路径统一经此调用。
     */
    virtual void accumulate(omnistream::VectorBatch* input, const std::vector<int>& indices)
    {
        for (int index : indices) {
            accumulate(input->extractRowData(index));
        }
    }

    /** 向量化批处理入口（回退版本，默认实现同上）。 */
    virtual void retract(omnistream::VectorBatch* input, const std::vector<int>& indices)
    {
        for (int index : indices) {
            retract(input->extractRowData(index));
        }
    }

    /**
     * 分组存活判断：成员账本为空即分组消亡（算子据此走 clear + cleanup 分支）。
     * 对齐 Flink recordCounter.recordCountIsZero 的职责；Top-N 表聚合以「榜上无人」为准。
     */
    virtual bool isGroupEmpty() const = 0;

    // ---- 表聚合核心增量接口（单值聚合契约没有的方法）----

    /**
     * 将当前分组的表聚合结果以 0..N 行发射给下游。
     *
     * isRetract=true  发射旧结果的回撤行（generateUpdateBefore 时 UPDATE_BEFORE，否则 DELETE）；
     * isRetract=false 发射新结果行（firstRow 时 INSERT，否则 UPDATE_AFTER）。
     * 每行 = currentKey + 聚合输出列；发射行数与次序由实现保证与原生 Flink 一致
     * （例如 Top-N 满员时为 N 行、按排序列有序）。
     *
     * 对齐 Flink 签名：emitValue(Collector<RowData> out, RowData currentKey, boolean isRetract)；
     * firstRow 为 OmniStream 增补参数（算子层已持有该判断，避免函数内重复推导），
     * 默认值保证三参调用兼容。
     */
    virtual void emitValue(TimestampedCollector* out, RowData* currentKey, bool isRetract, bool firstRow = false) = 0;
};

#endif // FLINK_TNEL_TABLE_AGGS_HANDLE_FUNCTION_H
