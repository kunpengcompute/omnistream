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

#include "VectorBatch.h"
#include <fstream>

#include "OmniOperatorJIT/core/src/codegen/time_util.h"
#include "data/binary/BinaryRowData.h"
#include "table/data/rowdata_marshaller.h"
#include "table/utils/VectorBatchSerializationUtils.h"

namespace {
std::string FormatDecimal(std::string valueStr, int32_t scale)
{
    if (scale <= 0) {
        return valueStr;
    }

    bool negative = !valueStr.empty() && valueStr.front() == '-';
    if (negative) {
        valueStr.erase(0, 1);
    }

    int len = static_cast<int>(valueStr.length());
    if (scale >= len) {
        valueStr = "0." + std::string(scale - len, '0') + valueStr;
    } else {
        valueStr.insert(len - scale, ".");
    }

    return negative ? "-" + valueStr : valueStr;
}
} // namespace

namespace omnistream {
VectorBatch::VectorBatch(size_t rowCnt)
    : omniruntime::vec::VectorBatch(rowCnt),
      timestamps(nullptr),
      rowKinds(nullptr),
      maxTimestamp(INT64_MIN)
{
    if (rowCnt > 0) {
        timestamps = new int64_t[rowCnt];
        memset_s(timestamps, sizeof(int64_t) * rowCnt, 0, sizeof(int64_t) * rowCnt);

        rowKinds = new RowKind[rowCnt];
        memset_s(rowKinds, sizeof(RowKind) * rowCnt, 0, sizeof(RowKind) * rowCnt);
    }
}

VectorBatch::~VectorBatch()
{
    delete[] timestamps;
    delete[] rowKinds;
}

VectorBatch::VectorBatch(
    std::unique_ptr<omniruntime::vec::VectorBatch> baseVecBatch, int64_t* timestamps, RowKind* rowkinds)
    : omniruntime::vec::VectorBatch(baseVecBatch->GetRowCount())
{
    auto baseVectors = baseVecBatch->GetVectors();
    const int32_t baseCount = baseVecBatch->GetVectorCount();
    if (baseVectors == nullptr && baseCount > 0) {
        THROW_LOGIC_EXCEPTION("VectorBatch: null vector array with count " + std::to_string(baseCount));
    }
    for (int32_t i = 0; i < baseCount; ++i) {
        if (baseVectors[i] == nullptr) {
            THROW_LOGIC_EXCEPTION(
                "VectorBatch: null vector at index " + std::to_string(i) + " of " + std::to_string(baseCount));
        }
    }
    this->vectors.insert(this->vectors.end(), baseVectors, baseVectors + baseCount);
    // The vectors are now owned by this batch. Clear the source so the unique_ptr
    // can be destroyed without deleting the transferred vectors.
    baseVecBatch->ClearVectors();
    this->rowKinds = rowkinds;
    this->timestamps = timestamps;
    this->maxTimestamp = INT64_MIN;
    refreshSizeInBytes();
}

void VectorBatch::refreshSizeInBytes()
{
    sizeInBytes_ = VectorBatchSerializationUtils::calculateVectorBatchPayloadSize(this);
}

int64_t VectorBatch::setMaxTimestamp(int colIdx)
{
    auto* col = reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(this->Get(colIdx));
    for (int i = 0; i < this->GetRowCount(); ++i) {
        maxTimestamp = std::max(maxTimestamp, col->GetValue(i));
    }
    return maxTimestamp;
}

void VectorBatch::RearrangeColumns(std::vector<int32_t>& inputIndices)
{
    LOG("=====>");
    std::vector<bool> toKeep(this->vectors.size(), false);
    std::vector<omniruntime::vec::BaseVector*> newVectors(inputIndices.size());
    for (size_t i = 0; i < inputIndices.size(); ++i) {
        newVectors[i] = this->vectors[inputIndices[i]];
        toKeep[inputIndices[i]] = true;
    }
    for (size_t i = 0; i < toKeep.size(); ++i) {
        if (!toKeep[i]) {
            delete vectors[i];
        }
    }
    this->vectors = newVectors;
    refreshSizeInBytes();
}

// The caller takes ownership of the returned pointer
RowData* VectorBatch::extractRowData(int rowIndex)
{
    if (rowIndex >= this->GetRowCount()) {
        return nullptr;
    }
    // Get the number of columns in the batch.
    int numColumns = this->GetVectorCount();
    // Create a new BinaryRowData with numColumns fields.
    // BinaryRowData is assumed to implement the RowData interface.
    BinaryRowData* outRow = BinaryRowData::createBinaryRowDataWithMem(numColumns);

    for (int colIndex = 0; colIndex < numColumns; ++colIndex) {
        auto col = this->Get(colIndex);
        if (col->IsNull(rowIndex)) {
            outRow->setNullAt(colIndex);
            continue;
        }
        auto typeId = col->GetTypeId();
        // It is a omniruntime type
        if (typeId < OMNI_INVALID && rowSerializerCenter[typeId] != nullptr) {
            rowSerializerCenter[typeId](col, rowIndex, outRow, colIndex);
        } else if (
            typeId == DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE ||
            typeId == omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
            rowSerializerCenter[OMNI_LONG](col, rowIndex, outRow, colIndex);
        } else {
            THROW_RUNTIME_ERROR("extractRowData Data type not supported: " << typeId);
        }
    }
    // Set the row kind using the row kinds stored in the batch.
    outRow->setRowKind(this->getRowKind(rowIndex));
    return outRow;
}

std::string VectorBatch::TransformTimeWithTimeZone(
    int vectorID, int rowID, const std::string& tzStr, int precision) const
{
    auto millis = reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(vectors[vectorID])->GetValue(rowID);
    int64_t adjusted_seconds = (millis >= 0) ? (millis / 1000) : ((millis - 999) / 1000);
    int milliseconds = millis % 1000;
    if (milliseconds < 0) {
        const int addTime = 1000;
        milliseconds += addTime; // 确保毫秒非负（如-1234ms → -2秒 + 766ms）
    }
    setenv("TZ", omniruntime::codegen::function::TimeZoneUtil::GetTZ(tzStr.c_str()), 1);
    tzset();
    struct tm timeinfo;
    localtime_r(&adjusted_seconds, &timeinfo);
    char buffer[80];
    strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", &timeinfo);
    std::ostringstream oss;
    oss << buffer << ".";

    if (precision <= 3) {
        oss << std::setw(3) << std::setfill('0') << milliseconds; // 强制3位宽度，不足补零
    } else if (precision <= 9) {
        oss << std::setw(3) << std::setfill('0') << milliseconds << std::string(precision - 3, '0');
    } else {
        oss << std::setw(3) << std::setfill('0') << milliseconds << std::string(6, '0');
    }

    std::string result = oss.str();
    return result;
}

std::string VectorBatch::TransformTime(int vectorID, int rowID, int precision) const
{
    auto millis = reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(vectors[vectorID])->GetValue(rowID);
    int64_t adjusted_seconds = (millis >= 0) ? (millis / 1000) : ((millis - 999) / 1000);
    int milliseconds = millis % 1000;
    if (milliseconds < 0) {
        const int addTime = 1000;
        milliseconds += addTime; // 确保毫秒非负（如-1234ms → -2秒 + 766ms）
    }

    struct tm timeinfo;
    gmtime_r(&adjusted_seconds, &timeinfo);

    // 格式化为字符串
    char buffer[80];
    strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", &timeinfo);

    std::ostringstream oss;
    oss << buffer << ".";

    if (precision <= 3) {
        // precision <= 3时，补齐到3位（毫秒精度）
        oss << std::setw(3) << std::setfill('0') // 强制3位宽度，不足补零
            << milliseconds;
    } else if (precision <= 9) {
        // 3 < precision <= 9时，输出毫秒部分并补0到precision位数
        oss << std::setw(3) << std::setfill('0') // 强制3位宽度，不足补零
            << milliseconds << std::string(precision - 3, '0');
    } else {
        // precision > 9时，截断到9位
        oss << std::setw(3) << std::setfill('0')    // 强制3位宽度，不足补零
            << milliseconds << std::string(6, '0'); // 补0到9位
    }

    std::string result = oss.str();
    return result;
}

std::string VectorBatch::TransformOnlyTime(int vectorID, int rowID, int precision) const
{
    auto millis = reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(vectors[vectorID])->GetValue(rowID);
    int64_t totalSeconds = millis / 1000;
    int milliseconds = millis % 1000;
    if (milliseconds < 0) {
        milliseconds += 1000; // 保证毫秒非负数
        totalSeconds -= 1;
    }
    int hours = totalSeconds / 3600; // 取出小时数、分钟数和秒数
    int minutes = (totalSeconds % 3600) / 60;
    int seconds = totalSeconds % 60;

    // 检测越界值（hours >= 24 会产生非法的 "24:00:00.000" 及以上输出）
    while (hours >= 24) {
        hours -= 24; // 将hour限定在0-23之间
    }

    char buf[32];
    std::snprintf(buf, sizeof(buf), "%02d:%02d:%02d", hours, minutes, seconds);

    std::ostringstream oss;
    oss << buf << ".";

    if (precision <= 3) {
        // precision <= 3时，补齐到3位（毫秒精度）
        oss << std::setw(3) << std::setfill('0') // 强制3位宽度，不足补零
            << milliseconds;
    } else if (precision <= 9) {
        // 3 < precision <= 9时，输出毫秒部分并补0到precision位数
        oss << std::setw(3) << std::setfill('0') // 强制3位宽度，不足补零
            << milliseconds << std::string(precision - 3, '0');
    } else {
        // precision > 9时，截断到9位
        oss << std::setw(3) << std::setfill('0')    // 强制3位宽度，不足补零
            << milliseconds << std::string(6, '0'); // 补0到9位
    }

    std::string result = oss.str();
    return result;
}

std::string VectorBatch::transformDecimal128(
    int vectorID, int rowID, std::vector<std::pair<int32_t, int32_t>>& decimalInfo) const
{
    std::string valueStr =
        (reinterpret_cast<omniruntime::vec::Vector<Decimal128>*>(vectors[vectorID])->GetValue(rowID)).ToString();
    if (static_cast<int>(decimalInfo.size()) > vectorID && decimalInfo[vectorID].second > 0) {
        valueStr = FormatDecimal(valueStr, decimalInfo[vectorID].second);
    }
    return valueStr;
}

std::string VectorBatch::transformDecimal64(
    int vectorID, int rowID, std::vector<std::pair<int32_t, int32_t>>& decimalInfo) const
{
    std::string valueStr =
        std::to_string(reinterpret_cast<omniruntime::vec::Vector<long>*>(vectors[vectorID])->GetValue(rowID));
    if (static_cast<int>(decimalInfo.size()) > vectorID && decimalInfo[vectorID].second > 0) {
        valueStr = FormatDecimal(valueStr, decimalInfo[vectorID].second);
    }
    return valueStr;
}

void VectorBatch::WriteString(std::ofstream& file, int vectorID, int rowID) const
{
    if (vectors[vectorID]->GetEncoding() == omniruntime::vec::OMNI_FLAT) {
        auto casted =
            reinterpret_cast<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(
                vectors[vectorID]);
        file << casted->GetValue(rowID);
    } else { // DICTIONARY
        auto casted = reinterpret_cast<omniruntime::vec::Vector<
            omniruntime::vec::DictionaryContainer<std::string_view, omniruntime::vec::LargeStringContainer>>*>(
            vectors[vectorID]);
        file << casted->GetValue(rowID);
    }
}

void VectorBatch::WriteToFileInternal(
    int vectorID,
    int rowID,
    std::ofstream& file,
    std::vector<std::pair<int32_t, int32_t>> decimalInfo,
    std::vector<std::string> inputTypes,
    const std::string& tzStr) const
{
    int dataId = vectors[vectorID]->GetTypeId();
    switch (dataId) {
        case omniruntime::type::DataTypeId::OMNI_TIME_WITHOUT_TIME_ZONE:
        case omniruntime::type::DataTypeId::OMNI_TIMESTAMP:
        case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
        case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        case omniruntime::type::DataTypeId::OMNI_LONG:
            LOG("vb writefile inputType is " << inputTypes[vectorID]);
            if (inputTypes[vectorID].substr(0, 30) == "TIMESTAMP_WITH_LOCAL_TIME_ZONE") {
                auto result = TransformTimeWithTimeZone(vectorID, rowID, tzStr);
                file << result;
            } else if (inputTypes[vectorID].substr(0, 9) == "TIMESTAMP") {
                int precision = 3;
                size_t parenPos = inputTypes[vectorID].find('(');
                if (parenPos != std::string::npos) {
                    size_t endParen = inputTypes[vectorID].find(')', parenPos);
                    if (endParen != std::string::npos) {
                        std::string precisionStr = inputTypes[vectorID].substr(parenPos + 1, endParen - parenPos - 1);
                        precision = std::stoi(precisionStr);
                    }
                }
                auto result = TransformTime(vectorID, rowID, precision);
                file << result;
            } else if (inputTypes[vectorID].substr(0, 22) == "TIME_WITHOUT_TIME_ZONE") {
                int precision = 3;
                size_t parenPos = inputTypes[vectorID].find('(');
                if (parenPos != std::string::npos) {
                    size_t endParen = inputTypes[vectorID].find(')', parenPos);
                    if (endParen != std::string::npos) {
                        std::string precisionStr = inputTypes[vectorID].substr(parenPos + 1, endParen - parenPos - 1);
                        precision = std::stoi(precisionStr);
                    }
                }
                auto result = TransformOnlyTime(vectorID, rowID, precision);
                file << result;
            } else {
                file << reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(vectors[vectorID])->GetValue(rowID);
            }
            break;
        case omniruntime::type::DataTypeId::OMNI_VARCHAR:
        case omniruntime::type::DataTypeId::OMNI_CHAR: WriteString(file, vectorID, rowID); break;
        case omniruntime::type::DataTypeId::OMNI_DOUBLE:
            file << reinterpret_cast<omniruntime::vec::Vector<double>*>(vectors[vectorID])->GetValue(rowID);
            break;
        case omniruntime::type::DataTypeId::OMNI_INT:
            file << reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(vectors[vectorID])->GetValue(rowID);
            break;
        case omniruntime::type::DataTypeId::OMNI_BOOLEAN:
            file << std::boolalpha
                 << reinterpret_cast<omniruntime::vec::Vector<bool>*>(vectors[vectorID])->GetValue(rowID);
            break;
        case omniruntime::type::DataTypeId::OMNI_DECIMAL64: {
            auto valueStr = transformDecimal64(vectorID, rowID, decimalInfo);
            file << valueStr;
            break;
        }
        case omniruntime::type::DataTypeId::OMNI_DECIMAL128: {
            auto valueStr = transformDecimal128(vectorID, rowID, decimalInfo);
            file << valueStr;
            break;
        }
        default: std::runtime_error("WriteToFileInternal data type not supported");
    }
}

void VectorBatch::writeToFile(
    std::string& filename,
    std::ios_base::openmode mode,
    std::vector<std::pair<int32_t, int32_t>> decimalInfo,
    std::vector<std::string> inputTypes,
    const std::string& tzStr) const
{
    std::ofstream file;
    if (!normalizeAndValidatePath(filename)) {
        std::cerr << "Error validating file\n";
        return;
    }

    file.open(filename, mode);
    if (!file.is_open()) {
        std::cerr << "Error opening file\n";
        return;
    }
    // Write row-wise
    std::vector<std::string> rowKindStr = {"+I", "-U", "+U", "-D"};
    for (size_t i = 0; i < rowCnt; ++i) {
        file << rowKindStr[(int)rowKinds[i]];
        for (size_t j = 0; j < vectors.size(); ++j) {
            file << ",";
            // write null or value
            if (vectors[j]->IsNull(i)) {
                file << "NULL";
            } else {
                WriteToFileInternal(j, i, file, decimalInfo, inputTypes, tzStr);
            }
        }
        file << "\n";
    }
    file.close();
    LOG("write file finish");
}

void VectorBatch::convertToJson(
    nlohmann::ordered_json& j,
    int rowIndex,
    std::vector<std::pair<int32_t, int32_t>> decimalInfo,
    std::vector<std::string> inputTypes,
    std::vector<std::string> inputFields) const
{
    for (size_t colIndex = 0; colIndex < vectors.size(); ++colIndex) {
        if (vectors[colIndex]->IsNull(rowIndex)) {
            j[inputFields[colIndex]] = nullptr;
            continue;
        }
        int dataId = vectors[colIndex]->GetTypeId();
        switch (dataId) {
            case omniruntime::type::DataTypeId::OMNI_TIME_WITHOUT_TIME_ZONE:
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP:
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case omniruntime::type::DataTypeId::OMNI_LONG: {
                if (inputTypes[colIndex].substr(0, 9) == "TIMESTAMP") {
                    auto result = RemoveTrailingZeros(TransformTime(colIndex, rowIndex));
                    j[inputFields[colIndex]] = result;
                } else if (inputTypes[colIndex].substr(0, 22) == "TIME_WITHOUT_TIME_ZONE") {
                    auto result = TransformOnlyTime(colIndex, rowIndex);
                    j[inputFields[colIndex]] = result;
                } else {
                    auto result =
                        reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(vectors[colIndex])->GetValue(rowIndex);
                    j[inputFields[colIndex]] = result;
                }
                break;
            }
            case omniruntime::type::DataTypeId::OMNI_VARCHAR:
            case omniruntime::type::DataTypeId::OMNI_CHAR: {
                if (vectors[colIndex]->GetEncoding() == omniruntime::vec::OMNI_FLAT) {
                    auto casted = reinterpret_cast<
                        omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(
                        vectors[colIndex]);
                    j[inputFields[colIndex]] = casted->GetValue(rowIndex);
                } else { // DICTIONARY
                    auto casted = reinterpret_cast<omniruntime::vec::Vector<omniruntime::vec::DictionaryContainer<
                        std::string_view,
                        omniruntime::vec::LargeStringContainer>>*>(vectors[colIndex]);
                    j[inputFields[colIndex]] = casted->GetValue(rowIndex);
                }
                break;
            }
            case omniruntime::type::DataTypeId::OMNI_DOUBLE: {
                auto result =
                    reinterpret_cast<omniruntime::vec::Vector<double>*>(vectors[colIndex])->GetValue(rowIndex);
                j[inputFields[colIndex]] = result;
                break;
            }
            case omniruntime::type::DataTypeId::OMNI_INT: {
                auto result =
                    reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(vectors[colIndex])->GetValue(rowIndex);
                j[inputFields[colIndex]] = result;
                break;
            }
            case omniruntime::type::DataTypeId::OMNI_BOOLEAN: {
                auto result = reinterpret_cast<omniruntime::vec::Vector<bool>*>(vectors[colIndex])->GetValue(rowIndex);
                j[inputFields[colIndex]] = result;
                break;
            }

            case omniruntime::type::DataTypeId::OMNI_DECIMAL64: {
                auto valueStr = transformDecimal64(colIndex, rowIndex, decimalInfo);
                j[inputFields[colIndex]] = valueStr;
                break;
            }
            case omniruntime::type::DataTypeId::OMNI_DECIMAL128: {
                auto valueStr = transformDecimal128(colIndex, rowIndex, decimalInfo);
                j[inputFields[colIndex]] = valueStr;
                break;
            }
            default: std::runtime_error("convertToJson data type not supported");
        }
    }
    LOG("convertToJson finish");
}

std::vector<XXH128_hash_t> VectorBatch::getXXH128s()
{
    std::vector<XXH128_hash_t> hashes(rowCnt);
    for (size_t i = 0; i < rowCnt; ++i) {
        XXH3_state_t* state = XXH3_createState();
        XXH3_128bits_reset(state);
        for (auto vec : vectors) {
            auto dataTypeId = vec->GetTypeId();
            switch (dataTypeId) {
                case OMNI_LONG:
                case OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
                case OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                case OMNI_TIMESTAMP: {
                    auto casted = reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(vec);
                    auto val = casted->GetValue(i);
                    XXH3_128bits_update(state, &val, sizeof(int64_t));
                    break;
                }
                case OMNI_VARCHAR:
                case OMNI_CHAR: {
                    if (vec->GetEncoding() == omniruntime::vec::OMNI_FLAT) {
                        auto casted = reinterpret_cast<
                            omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(vec);
                        auto val = casted->GetValue(i);
                        XXH3_128bits_update(state, val.data(), val.size());
                    } else {
                        auto casted = reinterpret_cast<omniruntime::vec::Vector<omniruntime::vec::DictionaryContainer<
                            std::string_view,
                            omniruntime::vec::LargeStringContainer>>*>(vec);
                        auto val = casted->GetValue(i);
                        XXH3_128bits_update(state, val.data(), val.size());
                    }
                    break;
                }
                default: XXH3_freeState(state); throw std::runtime_error("Type not supported yet");
            }
        }
        // Compute the final hash
        hashes[i] = XXH3_128bits_digest(state);
        XXH3_freeState(state);
    }
    return hashes;
}

omniruntime::vec::BaseVector* VectorBatch::CopyPositionsAndFlatten(
    omniruntime::vec::BaseVector* input, const int* positions, int offset, int length)
{
    if (input->GetTypeId() != omniruntime::type::OMNI_VARCHAR && input->GetTypeId() != omniruntime::type::OMNI_CHAR) {
        throw std::runtime_error("Type is not Varchar or Char");
    }
    if (input->GetEncoding() != omniruntime::vec::OMNI_DICTIONARY) {
        throw std::runtime_error("not dictionary");
    }
    // if it is a dictionary varchar vector, we copy the value out one by one and obtain a flat vector
    using DictVarcharVecType = omniruntime::vec::Vector<
        omniruntime::vec::DictionaryContainer<std::string_view, omniruntime::vec::LargeStringContainer>>;
    auto casted = reinterpret_cast<DictVarcharVecType*>(input);
    // new vector will be a flat one
    auto vector = new omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>(length);
    auto startPositions = positions + offset;
    for (int32_t i = 0; i < length; i++) {
        auto position = startPositions[i];
        if (input->IsNull(position)) {
            vector->SetNull(i);
        } else {
            auto value = casted->GetValue(position);
            vector->SetValue(i, value);
        }
    }
    return vector;
}

omnistream::VectorBatch* VectorBatch::CreateVectorBatch(int rowCount, const std::vector<DataTypeId>& dataTypes)
{
    // Build vector batch
    auto* vectorBatch = new omnistream::VectorBatch(rowCount);
    for (size_t i = 0; i < dataTypes.size(); i++) {
        switch (dataTypes[i]) {
            case (omniruntime::type::DataTypeId::OMNI_BOOLEAN): {
                auto vec = new omniruntime::vec::Vector<bool>(rowCount);
                vectorBatch->Append(vec);
                break;
            }
            case (omniruntime::type::DataTypeId::OMNI_INT): {
                auto vec = new omniruntime::vec::Vector<int32_t>(rowCount);
                vectorBatch->Append(vec);
                break;
            }
            case (omniruntime::type::DataTypeId::OMNI_TIME_WITHOUT_TIME_ZONE):
            case (omniruntime::type::DataTypeId::OMNI_LONG):
            case (omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE):
            case (omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE):
            case (omniruntime::type::DataTypeId::OMNI_TIMESTAMP): {
                auto vec = new omniruntime::vec::Vector<int64_t>(rowCount);
                vectorBatch->Append(vec);
                break;
            }
            case (omniruntime::type::DataTypeId::OMNI_CHAR):
            case (omniruntime::type::DataTypeId::OMNI_VARCHAR): {
                auto vec =
                    new omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>(rowCount);
                vectorBatch->Append(vec);
                break;
            }
            case (omniruntime::type::DataTypeId::OMNI_DECIMAL64): {
                auto vec =
                    new omniruntime::vec::Vector<int64_t>(rowCount, omniruntime::type::DataTypeId::OMNI_DECIMAL64);
                vectorBatch->Append(vec);
                break;
            }
            case (omniruntime::type::DataTypeId::OMNI_DECIMAL128): {
                auto vec = new omniruntime::vec::Vector<omniruntime::type::Decimal128>(rowCount);
                vectorBatch->Append(vec);
                break;
            }
            default:
                throw std::runtime_error(
                    "CreateVectorBatch: unsupported type id " + std::to_string(static_cast<int>(dataTypes[i])));
        }
    }
    return vectorBatch;
}

std::string VectorBatch::RemoveTrailingZeros(std::string num)
{
    size_t dot_pos = num.find('.');
    if (dot_pos == std::string::npos) {
        return num;
    }

    std::string integer_part = num.substr(0, dot_pos);
    std::string decimal_part = num.substr(dot_pos + 1);

    // 去除小数部分末尾的零
    size_t last_non_zero = decimal_part.find_last_not_of('0');
    if (last_non_zero != std::string::npos) {
        decimal_part = decimal_part.substr(0, last_non_zero + 1);
    } else {
        decimal_part.clear();
    }

    // 处理整数部分为空的情况（如输入是 ".500" → 转为 "0.500"）
    if (integer_part.empty()) {
        integer_part = "0";
    }

    // 组合结果
    if (decimal_part.empty()) {
        return integer_part;
    } else {
        return integer_part + "." + decimal_part;
    }
}
} // namespace omnistream
