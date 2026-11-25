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

#ifndef FLINK_TNEL_SPLITREADER_H
#define FLINK_TNEL_SPLITREADER_H

#include "RecordsWithSplitIds.h"
#include "connector/kafka/source/split/KafkaPartitionSplit.h"

template <typename E, typename SplitT>
class SplitReader {
public:
    // 纯虚函数，用于从数据源获取记录
    virtual RecordsWithSplitIds<E>* fetch() = 0;
    // 纯虚函数，用于处理分片变更
    virtual void handleSplitsChanges(const std::vector<SplitT*>& splitsChange) = 0;
    // 纯虚函数，用于唤醒读取操作
    virtual void wakeUp() = 0;
    // 纯虚函数，用于关闭读取器
    virtual void close() = 0;
    // 虚析构函数，确保正确释放派生类对象
    virtual ~SplitReader() = default;
};

#endif // FLINK_TNEL_SPLITREADER_H
