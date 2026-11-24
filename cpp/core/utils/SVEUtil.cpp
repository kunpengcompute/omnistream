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

#include "core/utils/SVEUtil.h"

#include <arm_sve.h>

#include "table/data/vectorbatch/VectorBatch.h"
#include "common.h"

namespace omnistream {
void SVEUtil::copyU8ArrayByIndices(const uint8_t* source, uint8_t* target, const std::vector<int32_t>& indices)
{
    const auto perCount = static_cast<int32_t>(svcntw());
    const auto totalCount = static_cast<int32_t>(indices.size());
    for (int32_t i = 0; i < totalCount; i += perCount) {
        auto pg = svwhilelt_b32_s32(i, totalCount);
        svint32_t indices_sve = svld1_s32(pg, indices.data() + i);
        // For uint8_t elements, element indices are equal to byte offsets.
        svint32_t sourceData_sve = svld1ub_gather_s32offset_s32(pg, source, indices_sve);
        svst1b_s32(pg, target + i, sourceData_sve);
    }
}

void SVEUtil::copyI64ArrayByIndices(const int64_t* source, int64_t* target, const std::vector<int32_t>& indices)
{
    const auto perCount = static_cast<int32_t>(svcntw());
    const auto totalCount = static_cast<int32_t>(indices.size());
    const auto halfPerCount = static_cast<int32_t>(svcntd());
    for (int32_t i = 0; i < totalCount; i += perCount) {
        auto pg = svwhilelt_b32_s32(i, totalCount);
        svint32_t indices_sve = svld1_s32(pg, indices.data() + i);
        svint64_t indicesLow_sve = svunpklo_s64(indices_sve);
        svint64_t indicesHigh_sve = svunpkhi_s64(indices_sve);

        auto pgLow = svwhilelt_b64_s32(i, totalCount);
        svint64_t sourceDataLow_sve = svld1_gather_s64index_s64(pgLow, source, indicesLow_sve);
        svst1_s64(pgLow, target + i, sourceDataLow_sve);

        auto pgHigh = svwhilelt_b64_s32(i + halfPerCount, totalCount);
        svint64_t sourceDataHigh_sve = svld1_gather_s64index_s64(pgHigh, source, indicesHigh_sve);
        svst1_s64(pgHigh, target + i + halfPerCount, sourceDataHigh_sve);
    }
}

void SVEUtil::extract16BitsFromU64ToI32(const uint64_t* source, int32_t* target, int32_t num, int32_t offset)
{
    if (offset < 0 || offset > 48) {
        THROW_RUNTIME_ERROR("offset must be in [0, 48]");
    }
    const auto perCount = static_cast<int32_t>(svcntw());
    const auto halfPerCount = static_cast<int32_t>(svcntd());

    svuint64_t offset_sve = svdup_n_u64(static_cast<uint64_t>(offset));

    for (int32_t i = 0; i < num; i += perCount) {
        svbool_t pgLow = svwhilelt_b64_s32(i, num);
        svuint64_t sourceLow_sve = svld1_u64(pgLow, source + i);
        svuint64_t extractedLow = svand_n_u64_x(pgLow, svlsr_u64_x(pgLow, sourceLow_sve, offset_sve), 0xffffULL);

        svbool_t pgHigh = svwhilelt_b64_s32(i + halfPerCount, num);
        svuint64_t sourceHigh_sve = svld1_u64(pgHigh, source + i + halfPerCount);
        svuint64_t extractedHigh = svand_n_u64_x(pgHigh, svlsr_u64_x(pgHigh, sourceHigh_sve, offset_sve), 0xffffULL);

        svuint32_t packed = svuzp1_u32(svreinterpret_u32_u64(extractedLow), svreinterpret_u32_u64(extractedHigh));
        svbool_t pgOut = svwhilelt_b32_s32(i, num);
        svst1_s32(pgOut, target + i, svreinterpret_s32_u32(packed));
    }
}

void SVEUtil::extract32BitsFromU64ToU32(const uint64_t* source, uint32_t* target, int32_t num, int32_t offset)
{
    if (offset < 0 || offset > 32) {
        THROW_RUNTIME_ERROR("offset must be in [0, 32]");
    }
    const auto perCount = static_cast<int32_t>(svcntw());
    const auto halfPerCount = static_cast<int32_t>(svcntd());

    svuint64_t offset_sve = svdup_n_u64(static_cast<uint64_t>(offset));

    for (int32_t i = 0; i < num; i += perCount) {
        svbool_t pgLow = svwhilelt_b64_s32(i, num);
        svuint64_t sourceLow_sve = svld1_u64(pgLow, source + i);
        svuint64_t extractedLow = svand_n_u64_x(pgLow, svlsr_u64_x(pgLow, sourceLow_sve, offset_sve), 0xffffffffULL);

        svbool_t pgHigh = svwhilelt_b64_s32(i + halfPerCount, num);
        svuint64_t sourceHigh_sve = svld1_u64(pgHigh, source + i + halfPerCount);
        svuint64_t extractedHigh =
            svand_n_u64_x(pgHigh, svlsr_u64_x(pgHigh, sourceHigh_sve, offset_sve), 0xffffffffULL);

        svuint32_t packed = svuzp1_u32(svreinterpret_u32_u64(extractedLow), svreinterpret_u32_u64(extractedHigh));
        svbool_t pgOut = svwhilelt_b32_s32(i, num);
        svst1_u32(pgOut, target + i, packed);
    }
}
} // namespace omnistream
