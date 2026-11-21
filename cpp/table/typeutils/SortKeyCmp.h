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

#ifndef OMNISTREAM_SORTKEYCMP_H
#define OMNISTREAM_SORTKEYCMP_H

#include <string_view>
#include <utility>
#include <vector>

#include "OmniOperatorJIT/core/src/type/data_type.h"
#include "core/include/common.h"
#include "table/data/RowData.h"

// Logical sort-key comparator. Mirrors the semantics of Flink's code-generated
// RecordComparator over the projected sort key: honors per-column ascending order
// and nulls-last/first. Two keys that compare neither less nor greater are treated
// as equivalent (strict weak ordering), which drives the std::map grouping and the
// byte iteration order of the persisted sorted map, so it must match Flink exactly.
class SortKeyCmp {
public:
    SortKeyCmp() = default;

    SortKeyCmp(std::vector<omniruntime::type::DataTypeId> typeIds,
               std::vector<bool> ascending,
               std::vector<bool> nullsIsLast)
        : typeIds_(std::move(typeIds)),
          ascending_(std::move(ascending)),
          nullsIsLast_(std::move(nullsIsLast))
    {
    }

    bool operator()(RowData* left, RowData* right) const
    {
        for (size_t i = 0; i < typeIds_.size(); ++i) {
            const int cmp = compareColumn(left, right, static_cast<int>(i));
            if (cmp != 0) {
                return cmp < 0;
            }
        }
        return false;
    }

private:
    // Returns -1/0/+1 for left vs right in the total order of one sort column. Nulls sit
    // at the requested end (nullsIsLast) independent of the ascending/descending flip;
    // the flip applies to the non-null value comparison only (mirrors Flink's codegen).
    int compareColumn(RowData* left, RowData* right, int idx) const
    {
        const bool leftNull = left->isNullAt(idx);
        const bool rightNull = right->isNullAt(idx);
        if (leftNull && rightNull) {
            return 0;
        }
        if (leftNull) {
            return nullsIsLast_[idx] ? 1 : -1;
        }
        if (rightNull) {
            return nullsIsLast_[idx] ? -1 : 1;
        }
        int cmp = 0;
        switch (typeIds_[idx]) {
            case omniruntime::type::DataTypeId::OMNI_INT:
            case omniruntime::type::DataTypeId::OMNI_DATE32: {
                const int lv = *left->getInt(idx);
                const int rv = *right->getInt(idx);
                cmp = lv < rv ? -1 : (lv > rv ? 1 : 0);
                break;
            }
            case omniruntime::type::DataTypeId::OMNI_LONG:
            case omniruntime::type::DataTypeId::OMNI_DECIMAL64:
            case omniruntime::type::DataTypeId::OMNI_TIME_WITHOUT_TIME_ZONE:
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP: {
                const int64_t lv = *left->getLong(idx);
                const int64_t rv = *right->getLong(idx);
                cmp = lv < rv ? -1 : (lv > rv ? 1 : 0);
                break;
            }
            case omniruntime::type::DataTypeId::OMNI_DECIMAL128: {
                const omniruntime::type::Decimal128* lv = left->getDecimal128(idx);
                const omniruntime::type::Decimal128* rv = right->getDecimal128(idx);
                cmp = *lv < *rv ? -1 : (*rv < *lv ? 1 : 0);
                delete lv;
                delete rv;
                break;
            }
            case omniruntime::type::DataTypeId::OMNI_DOUBLE: {
                const double lv = *left->getDouble(idx);
                const double rv = *right->getDouble(idx);
                cmp = lv < rv ? -1 : (lv > rv ? 1 : 0);
                break;
            }
            case omniruntime::type::DataTypeId::OMNI_BOOLEAN: {
                const bool lv = *left->getBool(idx);
                const bool rv = *right->getBool(idx);
                cmp = lv == rv ? 0 : (lv ? 1 : -1);
                break;
            }
            case omniruntime::type::DataTypeId::OMNI_CHAR:
            case omniruntime::type::DataTypeId::OMNI_VARCHAR: {
                const std::string_view lv = left->getStringView(idx);
                const std::string_view rv = right->getStringView(idx);
                cmp = lv == rv ? 0 : (lv < rv ? -1 : 1);
                break;
            }
            default:
                THROW_LOGIC_EXCEPTION("SortKeyCmp: unsupported sort key column type");
        }
        return ascending_[idx] ? cmp : -cmp;
    }

    std::vector<omniruntime::type::DataTypeId> typeIds_;
    std::vector<bool> ascending_;
    std::vector<bool> nullsIsLast_;
};

#endif // OMNISTREAM_SORTKEYCMP_H
