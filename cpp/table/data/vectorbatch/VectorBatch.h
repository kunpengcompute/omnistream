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

#ifndef FLINK_TNEL_VECTORBATCH_H
#define FLINK_TNEL_VECTORBATCH_H
#include <fstream>
#include <iostream>
#include <memory>
#include <vector/vector_helper.h>

#include "OmniOperatorJIT/core/src/vector/vector_batch.h"
#include <xxhash.h>
#include "table/data/RowKind.h"
#include "core/include/common.h"
#include "data/RowData.h"
#include <iostream>
#include <filesystem>

namespace fs = std::filesystem;
using namespace omniruntime::type;
namespace omnistream {
class VectorBatch : public omniruntime::vec::VectorBatch {
public:
    explicit VectorBatch(size_t rowCnt);

    // Takes ownership of baseVecBatch, timestamps and rowkinds.
    VectorBatch(std::unique_ptr<omniruntime::vec::VectorBatch> baseVecBatch, int64_t* timestamps, RowKind* rowkinds);

    ~VectorBatch();

    void setTimestamp(size_t rowIndex, int64_t timestampValue)
    {
        timestamps[rowIndex] = timestampValue;
    }

    void setTimestamps(size_t start, int64_t* data, size_t len)
    {
        memcpy_s(timestamps, len, data, len);
    }

    int64_t getTimestamp(size_t rowIndex)
    {
        return timestamps[rowIndex];
    }

    int64_t* getTimestamps()
    {
        return timestamps;
    }

    void setRowKind(size_t rowIndex, RowKind rowKind)
    {
        rowKinds[rowIndex] = rowKind;
    }

    void setRowKinds(size_t start, RowKind* data, size_t len)
    {
        memcpy_s(rowKinds, len, data, len);
    }

    RowKind getRowKind(size_t rowIndex)
    {
        return rowKinds[rowIndex];
    }

    RowKind* getRowKinds()
    {
        return rowKinds;
    }

    void releaseTimestampsAndRowKinds()
    {
        timestamps = nullptr;
        rowKinds = nullptr;
    }

    int32_t getSizeInBytes() const
    {
        return sizeInBytes_;
    }

    void Append(omniruntime::vec::BaseVector* vector)
    {
        omniruntime::vec::VectorBatch::Append(vector);
        refreshSizeInBytes();
    }

    void ClearVectors()
    {
        omniruntime::vec::VectorBatch::ClearVectors();
    }

    void FreeAllVectors()
    {
        omniruntime::vec::VectorBatch::FreeAllVectors();
    }

    RowData* extractRowData(int rowIndex);

    [[nodiscard]] std::vector<XXH128_hash_t> getXXH128s();
    bool isEmpty(int64_t currentTimestamp) const
    {
        return maxTimestamp != INT64_MIN && currentTimestamp > maxTimestamp;
    }

    int64_t setMaxTimestamp(int col);

    // adjust the position of columns, achieving a simple column projection
    void RearrangeColumns(std::vector<int32_t>& inputIndices);

    template <typename Type>
    Type GetValueAt(int32_t column, int32_t row)
    {
        return reinterpret_cast<omniruntime::vec::Vector<Type>*>(this->Get(column))->GetValue(row);
    }

    template <typename Type>
    void SetValueAt(int32_t column, int32_t row, Type value)
    {
        reinterpret_cast<omniruntime::vec::Vector<Type>*>(this->Get(column))->SetValue(row, value);
    }
    // write to file. The default mode is to open a new one
    void writeToFile(
        std::string& filename,
        std::ios_base::openmode mode = std::ios::out,
        std::vector<std::pair<int32_t, int32_t>> decimalInfo = {},
        std::vector<std::string> inputTypes = {},
        const std::string& tzStr = "Asia/Shanghai") const;
    void convertToJson(
        nlohmann::ordered_json& j,
        int rowIndex,
        std::vector<std::pair<int32_t, int32_t>> decimalInfo,
        std::vector<std::string> inputTypes,
        std::vector<std::string> inputFields) const;

    std::string TransformTime(int vectorID, int rowID, int precision = 3) const;
    std::string TransformOnlyTime(int vectorID, int rowID, int precision = 3) const;
    std::string TransformTimeWithTimeZone(int vectorID, int rowID, const std::string& tzStr, int precision = 3) const;
    std::string transformDecimal128(
        int vectorID, int rowID, std::vector<std::pair<int32_t, int32_t>>& decimalInfo) const;
    std::string transformDecimal64(
        int vectorID, int rowID, std::vector<std::pair<int32_t, int32_t>>& decimalInfo) const;
    void WriteToFileInternal(
        int vectorID,
        int rowID,
        std::ofstream& file,
        std::vector<std::pair<int32_t, int32_t>> decimalInfo,
        std::vector<std::string> inputTypes,
        const std::string& tzStr) const;
    void WriteString(std::ofstream& file, int vectorID, int rowID) const;
    /**
     * VectorHelper::CopyPositionsVector 构造副本时不传 dataTypeId，int64 承载的类型会退化成
     * OMNI_LONG(2)。断链时批量要经 VectorBatch::copy 深拷贝，DECIMAL64 列因此丢失逻辑类型；
     * 这里对 DECIMAL64 逐位置拷贝并保留类型 id（DECIMAL128 的模板默认 id 恰好正确）。
     */
    static omniruntime::vec::BaseVector* CopyPositionsKeepLogicalType(
        omniruntime::vec::BaseVector* src, int* positions, int offset, int length)
    {
        if (src == nullptr || src->GetTypeId() != OMNI_DECIMAL64 ||
            src->GetEncoding() == omniruntime::vec::OMNI_DICTIONARY) {
            return omniruntime::vec::VectorHelper::CopyPositionsVector(src, positions, offset, length);
        }
        auto* srcVector = reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(src);
        auto* copiedVector = new omniruntime::vec::Vector<int64_t>(length, OMNI_DECIMAL64);
        for (int i = 0; i < length; i++) {
            int32_t position = positions[offset + i];
            if (srcVector->IsNull(position)) {
                copiedVector->SetNull(i);
                continue;
            }
            copiedVector->SetValue(i, srcVector->GetValue(position));
        }
        return copiedVector;
    }

    VectorBatch* copy()
    {
        auto value = this;
        int32_t rowCount = value->GetRowCount();
        auto copiedVectorBatch = new VectorBatch(rowCount);
        int32_t vectorCount = value->GetVectorCount();

        std::vector<int> offsets(rowCount);
        std::iota(offsets.begin(), offsets.end(), 0);

        for (int i = 0; i < vectorCount; i++) {
            copiedVectorBatch->Append(CopyPositionsKeepLogicalType(value->Get(i), offsets.data(), 0, offsets.size()));
        }
        if (rowCount > 0) {
            memcpy_s(
                copiedVectorBatch->getTimestamps(),
                sizeof(int64_t) * rowCount,
                value->getTimestamps(),
                sizeof(int64_t) * rowCount);
            memcpy_s(
                copiedVectorBatch->getRowKinds(),
                sizeof(RowKind) * rowCount,
                value->getRowKinds(),
                sizeof(RowKind) * rowCount);
        }
        return copiedVectorBatch;
    }
    static omniruntime::vec::BaseVector* CopyPositionsAndFlatten(
        omniruntime::vec::BaseVector* input, const int* positions, int offset, int length);

    static omnistream::VectorBatch* CreateVectorBatch(int rowCnt, const std::vector<DataTypeId>& dataTypes);

    static std::string RemoveTrailingZeros(std::string num);

private:
    void refreshSizeInBytes();
    int64_t* timestamps;
    RowKind* rowKinds;
    int64_t maxTimestamp;
    int32_t sizeInBytes_ = 0;
    bool normalizeAndValidatePath(std::string& filePath) const
    {
        // 1. 检查路径是否为空
        if (filePath.empty()) {
            std::cerr << "Error: Path is empty" << std::endl;
            return false;
        }

        try {
            fs::path pathObj(filePath);
            if (!pathObj.is_absolute()) {
                pathObj = fs::absolute(pathObj);
            }
            filePath = static_cast<std::string>(pathObj.string());
            return true;
        } catch (const fs::filesystem_error& e) {
            // 捕获文件系统相关的错误（例如，无效路径或无权限）
            std::cerr << "Error: Filesystem error - " << e.what() << std::endl;
            return false;
        } catch (const std::exception& e) {
            std::cerr << "Error: " << e.what() << std::endl;
            return false;
        }
    }
};
} // namespace omnistream

#endif
