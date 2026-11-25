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

#ifndef LONG_SIZE_GAUGE_H
#define LONG_SIZE_GAUGE_H
#include <cstdint>
#include <functional>
#include "Gauge.h"

namespace omnistream {
// 64-bit counterpart of SizeGauge for byte-valued metrics (keyed-state data sizes)
// that can exceed INT_MAX. SizeGauge stays int (Flink convention, shared by counts/buffer pools).
class LongSizeGauge : public Gauge<int64_t> {
public:
    using LongSizeSupplier = std::function<int64_t()>;
    LongSizeGauge();
    void RegisterSupplier(LongSizeSupplier supplier);
    int64_t GetValue() const override;

private:
    LongSizeSupplier supplier;
};
} // namespace omnistream
#endif // LONG_SIZE_GAUGE_H
