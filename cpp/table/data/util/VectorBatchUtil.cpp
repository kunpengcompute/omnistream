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

#include "VectorBatchUtil.h"

#include <arm_sve.h>
#include "core/utils/SVEUtil.h"

namespace omnistream {
void VectorBatchUtil::decodeComboIds(
    const std::vector<ComboId>& comboIds,
    std::vector<int32_t>& keyGroups,
    std::vector<uint32_t>& sequenceNumbers,
    std::vector<int32_t>& rowIds)
{
    const auto perCount = static_cast<int32_t>(svcntw());
    const auto halfPerCount = static_cast<int32_t>(svcntd());
    const auto num = static_cast<int32_t>(comboIds.size());

    for (int32_t i = 0; i < num; i += perCount) {
        svbool_t pgLow = svwhilelt_b64_s32(i, num);
        svbool_t pgHigh = svwhilelt_b64_s32(i + halfPerCount, num);
        svbool_t pgOut = svwhilelt_b32_s32(i, num);

        svuint64_t comboIdsLow = svld1_u64(pgLow, comboIds.data() + i);
        svuint64_t comboIdsHigh = svld1_u64(pgHigh, comboIds.data() + i + halfPerCount);

        svuint64_t rowIdsLow = svand_n_u64_x(pgLow, comboIdsLow, UINT16_MAX);
        svuint64_t rowIdsHigh = svand_n_u64_x(pgHigh, comboIdsHigh, UINT16_MAX);

        svuint64_t sequenceNumbersLow = svand_n_u64_x(pgLow, svlsr_n_u64_x(pgLow, comboIdsLow, 16), UINT32_MAX);
        svuint64_t sequenceNumbersHigh = svand_n_u64_x(pgHigh, svlsr_n_u64_x(pgHigh, comboIdsHigh, 16), UINT32_MAX);

        svuint64_t keyGroupsLow = svlsr_n_u64_x(pgLow, comboIdsLow, 48);
        svuint64_t keyGroupsHigh = svlsr_n_u64_x(pgHigh, comboIdsHigh, 48);

        svuint32_t rowIdsPacked = svuzp1_u32(svreinterpret_u32_u64(rowIdsLow), svreinterpret_u32_u64(rowIdsHigh));
        svuint32_t sequenceNumbersPacked =
            svuzp1_u32(svreinterpret_u32_u64(sequenceNumbersLow), svreinterpret_u32_u64(sequenceNumbersHigh));
        svuint32_t keyGroupsPacked =
            svuzp1_u32(svreinterpret_u32_u64(keyGroupsLow), svreinterpret_u32_u64(keyGroupsHigh));

        svst1_s32(pgOut, rowIds.data() + i, svreinterpret_s32_u32(rowIdsPacked));
        svst1_u32(pgOut, sequenceNumbers.data() + i, sequenceNumbersPacked);
        svst1_s32(pgOut, keyGroups.data() + i, svreinterpret_s32_u32(keyGroupsPacked));
    }
}

void VectorBatchUtil::getComboId_sve(int32_t keyGroup, uint32_t sequenceNumber, int32_t rowCount, ComboId* result)
{
    if (keyGroup < 0 || keyGroup > UINT16_MAX) {
        THROW_RUNTIME_ERROR("keyGroup out of range");
    }

    const int32_t processNum = static_cast<int32_t>(svcntd());
    svuint64_t batchData = svorr_x(
        svptrue_b64(),
        svlsl_n_u64_x(svptrue_b64(), svdup_n_u64(static_cast<uint64_t>(keyGroup)), 48),
        svlsl_n_u64_x(svptrue_b64(), svdup_n_u64(static_cast<uint64_t>(sequenceNumber)), 16));
    for (int32_t rowId = 0; rowId < rowCount; rowId += processNum) {
        svbool_t pg = svwhilelt_b64(rowId, rowCount);
        svuint64_t rowData = svreinterpret_u64_s64(svindex_s64(rowId, 1));
        svuint64_t comboIds = svorr_z(pg, batchData, rowData);
        svst1_u64(pg, result + rowId, comboIds);
    }
}

VectorBatch* VectorBatchUtil::buildNewVectorBatchByRowIds(VectorBatch* vectorBatch, std::vector<int32_t>& rowIds)
{
    if (vectorBatch == nullptr) {
        THROW_RUNTIME_ERROR("vectorBatch cannot be nullptr");
    }

    auto result = new VectorBatch(rowIds.size());
    for (int32_t i = 0; i < vectorBatch->GetVectorCount(); ++i) {
        result->Append(copyVectorByRowIds(vectorBatch, i, rowIds));
    }

    SVEUtil::copyU8ArrayByIndices(
        reinterpret_cast<uint8_t*>(vectorBatch->getRowKinds()),
        reinterpret_cast<uint8_t*>(result->getRowKinds()),
        rowIds);

    SVEUtil::copyI64ArrayByIndices(vectorBatch->getTimestamps(), result->getTimestamps(), rowIds);
    return result;
}
} // namespace omnistream
