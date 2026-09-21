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

#pragma once

#include <cstdint>
#include <limits>
#include <numeric>
#include <vector>
#include "table/data/vectorbatch/VectorBatch.h"
#include "OmniOperatorJIT/core/src/vector/unsafe_vector.h"
#include "OmniOperatorJIT/core/src/vector/vector_helper.h"
#include "table/data/vectorbatch/VectorBatchStorageInfo.h"

namespace omnistream {
class VectorBatchUtil {
public:
    // keep getBatchId,getRowId, getComboId for Window Agg
    static inline int32_t getBatchId(int64_t id)
    {
        uint64_t uid = static_cast<uint64_t>(id);
        return (int32_t)(uid >> 32);
    }

    static inline int32_t getRowId(int64_t id)
    {
        return (int32_t)id;
    }

    static inline int64_t getComboId(int batchId, int rowId)
    {
        uint64_t ubatchId = static_cast<uint64_t>(batchId);
        uint32_t urowId = static_cast<uint32_t>(rowId);
        ubatchId = (ubatchId << 32) | urowId;
        return static_cast<int64_t>(ubatchId);
    }

    static inline int32_t getKeyGroup(ComboId comboId)
    {
        return static_cast<int32_t>((comboId >> 48) & UINT16_MAX);
    }

    static inline uint32_t getSequenceNumber(ComboId comboId)
    {
        return static_cast<uint32_t>((comboId >> 16) & UINT32_MAX);
    }

    static inline int32_t getRowId(ComboId comboId)
    {
        return static_cast<int32_t>(comboId & UINT16_MAX);
    }

    static inline VectorBatchId getVectorBatchId(int32_t keyGroup, uint32_t sequenceNumber)
    {
        if (keyGroup < 0 || keyGroup > UINT16_MAX) {
            THROW_RUNTIME_ERROR("keyGroup out of range");
        }

        auto ukeyGroup = static_cast<uint64_t>(keyGroup);
        auto usequenceNumber = static_cast<uint64_t>(sequenceNumber);
        return (ukeyGroup << 48) | (usequenceNumber << 16);
    }

    static inline VectorBatchId getVectorBatchId(ComboId comboId)
    {
        return (comboId >> 16) << 16;
    }

    static inline ComboId getComboId(int32_t keyGroup, uint32_t sequenceNumber, int32_t rowId)
    {
        auto vectorBatchId = getVectorBatchId(keyGroup, sequenceNumber);

        return getComboId(vectorBatchId, rowId);
    }

    static inline ComboId getComboId(omnistream::VectorBatchId vectorBatchId, int32_t rowId)
    {
        if ((vectorBatchId & UINT16_MAX) != 0) {
            THROW_RUNTIME_ERROR("the rowId part of VectorBatchId should be 0");
        }
        if (rowId < 0 || rowId > UINT16_MAX) {
            THROW_RUNTIME_ERROR("rowId out of range");
        }

        return vectorBatchId | static_cast<uint64_t>(rowId);
    }

    static void decodeComboIds(
        const std::vector<ComboId>& comboIds,
        std::vector<int32_t>& keyGroups,
        std::vector<uint32_t>& sequenceNumbers,
        std::vector<int32_t>& rowIds);

    static void getComboId_sve(int32_t keyGroup, uint32_t sequenceNumber, int32_t rowCount, ComboId* result);

    // The caller takes ownership of the returned pointer
    static omnistream::VectorBatch* sliceVectorBatch(omnistream::VectorBatch* batch, int32_t offset, int32_t newRowCnt)
    {
        if (batch == nullptr || newRowCnt <= 0) {
            LOG("Warning: split batch count is not valid.");
            return nullptr;
        }

        auto* slicedBatch = new omnistream::VectorBatch(newRowCnt);
        std::vector<int> offsets(batch->GetRowCount());
        std::iota(offsets.begin(), offsets.end(), 0);

        for (int k = 0; k < batch->GetVectorCount(); k++) {
            slicedBatch->Append(
                omniruntime::vec::VectorHelper::CopyPositionsVector(batch->Get(k), offsets.data(), offset, newRowCnt));
        }

        for (int j = 0; j < newRowCnt; j++) {
            slicedBatch->setTimestamp(j, batch->getTimestamp(j + offset));
            slicedBatch->setRowKind(j, batch->getRowKind(j + offset));
        }
        return slicedBatch;
    }

    // The caller takes ownership of the returned pointer
    static omnistream::VectorBatch* buildNewVectorBatchByRowIds(
        omnistream::VectorBatch* vectorBatch, std::vector<int32_t>& rowIds);

    /**
     * Copy selected elements from one vector in a VectorBatch into a new vector by row ids.
     *
     * @param vectorBatch Source VectorBatch that owns the vector to copy from.
     * @param vectorIndex Index of the source vector inside vectorBatch.
     * @param rowIds Row ids to copy from the source vector.
     * @return A newly allocated vector containing the selected elements. The caller takes ownership of the returned
     * pointer.
     */
    static omniruntime::vec::BaseVector* copyVectorByRowIds(
        omnistream::VectorBatch* vectorBatch, int32_t vectorIndex, std::vector<int32_t>& rowIds)
    {
        omniruntime::vec::BaseVector* result = nullptr;
        if (vectorBatch->Get(vectorIndex)->GetEncoding() == omniruntime::vec::OMNI_FLAT ||
            (vectorBatch->Get(vectorIndex)->GetTypeId() != omniruntime::type::OMNI_CHAR &&
             vectorBatch->Get(vectorIndex)->GetTypeId() != omniruntime::type::OMNI_VARCHAR)) {
            // The original vector is not varchar or it is varchar but flat
            result = omnistream::VectorBatchUtil::CopyPositionsVector(
                vectorBatch->Get(vectorIndex), rowIds.data(), 0, static_cast<int32_t>(rowIds.size()));
        } else {
            // It is a varchar dictionary, copy it out
            result = omnistream::VectorBatch::CopyPositionsAndFlatten(
                vectorBatch->Get(vectorIndex), rowIds.data(), 0, static_cast<int32_t>(rowIds.size()));
        }
        return result;
    }

    // the main body of this function is copied from vector_helper.h in OmniOperator
    static omniruntime::vec::BaseVector* CopyPositionsVector(
        omniruntime::vec::BaseVector* vector, int* positions, int offset, int length)
    {
        if (vector->GetEncoding() == omniruntime::vec::OMNI_DICTIONARY) {
            return omniruntime::vec::VectorHelper::CopyPositionsDictionaryVector(vector, positions, offset, length);
        }
        DataTypeId dataTypeId = vector->GetTypeId();
        switch (dataTypeId) {
            case omniruntime::type::OMNI_INT:
            case omniruntime::type::OMNI_DATE32: {
                return reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(vector)->CopyPositions(
                    positions, offset, length);
            }
            case omniruntime::type::OMNI_SHORT: {
                return reinterpret_cast<omniruntime::vec::Vector<int16_t>*>(vector)->CopyPositions(
                    positions, offset, length);
            }
            case omniruntime::type::OMNI_BYTE: {
                return reinterpret_cast<omniruntime::vec::Vector<int8_t>*>(vector)->CopyPositions(
                    positions, offset, length);
            }
            case omniruntime::type::OMNI_DECIMAL64: {
                // Vector<int64_t>::CopyPositions 不传 dataTypeId，DECIMAL64 会退化成
                // OMNI_LONG(2)；这里逐位置拷贝并显式保留 OMNI_DECIMAL64。
                if (UNLIKELY((positions == nullptr) || (length < 0))) {
                    throw omniruntime::vec::OmniException(
                        "OPERATOR_RUNTIME_ERROR", "positions is null or the input length is incorrect.");
                }
                auto* srcVector = reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(vector);
                auto* copiedVector = new omniruntime::vec::Vector<int64_t>(length, omniruntime::type::OMNI_DECIMAL64);
                for (int32_t i = 0; i < length; i++) {
                    int32_t position = positions[offset + i];
                    if (srcVector->IsNull(position)) {
                        copiedVector->SetNull(i);
                        continue;
                    }
                    copiedVector->SetValue(i, srcVector->GetValue(position));
                }
                return copiedVector;
            }
            case omniruntime::type::OMNI_LONG:
            case omniruntime::type::OMNI_TIMESTAMP:
            case omniruntime::type::OMNI_DATE64: {
                return reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(vector)->CopyPositions(
                    positions, offset, length);
            }
            case omniruntime::type::OMNI_DOUBLE: {
                return reinterpret_cast<omniruntime::vec::Vector<double>*>(vector)->CopyPositions(
                    positions, offset, length);
            }
            case omniruntime::type::OMNI_BOOLEAN: {
                return reinterpret_cast<omniruntime::vec::Vector<bool>*>(vector)->CopyPositions(
                    positions, offset, length);
            }
            case omniruntime::type::OMNI_VARCHAR:
            case omniruntime::type::OMNI_CHAR: {
                if (UNLIKELY((positions == nullptr) || (length < 0))) {
                    std::string message("positions is null or the input length is incorrect: %d.", length);
                    throw omniruntime::vec::OmniException("OPERATOR_RUNTIME_ERROR", message);
                }
                auto src = reinterpret_cast<
                    omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(vector);
                auto startPositions = positions + offset;

                constexpr size_t maxStringContainerCapacity = static_cast<size_t>(std::numeric_limits<int>::max());
                size_t totalBytes = 0;
                for (int32_t i = 0; i < length; i++) {
                    auto position = startPositions[i];
                    if (!vector->IsNull(position)) {
                        auto valueSize = src->GetValue(position).size();
                        if (UNLIKELY(valueSize > maxStringContainerCapacity - totalBytes)) {
                            throw omniruntime::vec::OmniException(
                                "OPERATOR_RUNTIME_ERROR", "string container capacity exceeds the supported limit");
                        }
                        totalBytes += valueSize;
                    }
                }

                auto dest = new omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>(
                    length, static_cast<int>(totalBytes));
                for (int32_t i = 0; i < length; i++) {
                    auto position = startPositions[i];
                    if (vector->IsNull(position)) {
                        dest->SetNull(i);

                    } else {
                        auto value = src->GetValue(position);
                        dest->SetValue(i, value);
                    }
                }
                return dest;
            }
            case omniruntime::type::OMNI_DECIMAL128: {
                return reinterpret_cast<omniruntime::vec::Vector<omniruntime::type::Decimal128>*>(vector)
                    ->CopyPositions(positions, offset, length);
            }
            case omniruntime::type::OMNI_CONTAINER:
                return reinterpret_cast<omniruntime::vec::ContainerVector*>(vector)->CopyPositions(
                    positions, offset, length);
            default: {
                std::string omniExceptionInfo =
                    "In function CopyPositionsVector, no such data type " + std::to_string(dataTypeId);
                throw omniruntime::exception::OmniException("UNSUPPORTED_ERROR", omniExceptionInfo);
            }
        }
    }

    // To print VectorBatch, use VectorHelper::PrintVecBatch from OmniOperatorJIT/core/src/vector/vector_helper.h

    template <typename fromType, typename returnType>
    static omniruntime::vec::BaseVector* castVectorTo(omniruntime::vec::BaseVector* vec)
    {
        auto data = omniruntime::vec::unsafe::UnsafeVector::GetRawValues(
            reinterpret_cast<omniruntime::vec::Vector<fromType>*>(vec));
        auto numRows = vec->GetSize();
        returnType* dataCopy = new returnType[numRows];
        for (int i = 0; i < numRows; i++) {
            if constexpr (std::is_same<returnType, Decimal128>::value) {
                dataCopy[i] = Decimal128(static_cast<int128_t>(data[i]));
            } else {
                dataCopy[i] = static_cast<returnType>(data[i]);
            }
        }
        omniruntime::vec::Vector<returnType>* newVector = new omniruntime::vec::Vector<returnType>(numRows);
        for (int32_t i = 0; i < numRows; i++) {
            newVector->SetValue(i, dataCopy[i]);
        }
        delete[] dataCopy;
        return newVector;
    }

    static std::string getValueAtAsStr(
        omnistream::VectorBatch* vecBatch, int32_t col, int32_t row, std::vector<std::string> inputTypes = {})
    {
        std::string value;
        if (vecBatch->Get(col)->IsNull(row)) {
            value = "NULL";
        } else {
            int dataId = vecBatch->Get(col)->GetTypeId();
            switch (dataId) {
                case omniruntime::type::DataTypeId::OMNI_LONG:
                    if (static_cast<int32_t>(inputTypes.size()) > col && inputTypes[col].substr(0, 9) == "TIMESTAMP") {
                        auto millis =
                            reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(vecBatch->Get(col))->GetValue(row);
                        return formatTimestamp(millis);
                    } else {
                        value = std::to_string(
                            reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(vecBatch->Get(col))->GetValue(row));
                    }
                    break;
                case omniruntime::type::DataTypeId::OMNI_VARCHAR:
                case omniruntime::type::DataTypeId::OMNI_CHAR: {
                    if (vecBatch->Get(col)->GetEncoding() == omniruntime::vec::OMNI_FLAT) {
                        auto casted = reinterpret_cast<
                            omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(
                            vecBatch->Get(col));
                        value = casted->GetValue(row);
                    } else {
                        auto casted = reinterpret_cast<omniruntime::vec::Vector<omniruntime::vec::DictionaryContainer<
                            std::string_view,
                            omniruntime::vec::LargeStringContainer>>*>(vecBatch->Get(col));
                        value = casted->GetValue(row);
                    }
                    break;
                }
                case omniruntime::type::DataTypeId::OMNI_DOUBLE:
                    value = std::to_string(
                        reinterpret_cast<omniruntime::vec::Vector<double>*>(vecBatch->Get(col))->GetValue(row));
                    break;
                case omniruntime::type::DataTypeId::OMNI_INT:
                    value = std::to_string(
                        reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(vecBatch->Get(col))->GetValue(row));
                    break;
                case omniruntime::type::DataTypeId::OMNI_BOOLEAN:
                    value = reinterpret_cast<omniruntime::vec::Vector<bool>*>(vecBatch->Get(col))->GetValue(row)
                                ? "true"
                                : "false";
                    break;
                case omniruntime::type::DataTypeId::OMNI_DECIMAL64:
                    value = std::to_string(
                        reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(vecBatch->Get(col))->GetValue(row));
                    break;
                case omniruntime::type::DataTypeId::OMNI_DECIMAL128:
                    value = reinterpret_cast<omniruntime::vec::Vector<Decimal128>*>(vecBatch->Get(col))
                                ->GetValue(row)
                                .ToString();
                    break;
                default: value = "Unsupported Data Type"; break;
            }
        }
        return value;
    }

    static std::string formatTimestamp(int64_t millis)
    {
        int milliseconds = millis % 1000;
        time_t seconds = millis / 1000;
        struct tm* timeinfo = gmtime(&seconds);
        char buffer[80];
        size_t len = strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", timeinfo);
        if (len == 0) {
            return "Invalid Time";
        }
        std::ostringstream oss;
        constexpr int millisWidth = 3;
        oss << buffer << "." << std::setw(millisWidth) << std::setfill('0') << milliseconds;
        return oss.str();
    }
};
} // namespace omnistream
