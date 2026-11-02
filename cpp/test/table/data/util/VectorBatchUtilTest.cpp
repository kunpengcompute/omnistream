#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "table/data/util/VectorBatchUtil.h"

using omnistream::VectorBatchUtil;

TEST(VectorBatchUtilTest, CopyFlatVectorFromVectorBatch)
{
    constexpr int32_t rowCount = 6;
    omnistream::VectorBatch vectorBatch(rowCount);
    auto* input = new omniruntime::vec::Vector<int32_t>(rowCount);
    for (int32_t i = 0; i < rowCount; ++i) {
        input->SetValue(i, 100 + i * 10);
    }
    input->SetNull(2);
    vectorBatch.Append(input);

    std::vector<int32_t> positions = {4, 2, 4, 0};
    auto* resultBase = VectorBatchUtil::copyVectorByRowIds(&vectorBatch, 0, positions);
    auto* result = reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(resultBase);

    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->GetSize(), static_cast<int32_t>(positions.size()));
    EXPECT_EQ(result->GetEncoding(), omniruntime::vec::OMNI_FLAT);
    EXPECT_EQ(result->GetValue(0), 140);
    EXPECT_TRUE(result->IsNull(1));
    EXPECT_EQ(result->GetValue(2), 140);
    EXPECT_EQ(result->GetValue(3), 100);

    vectorBatch.ClearVectors();
    delete result;
    delete input;
}

TEST(VectorBatchUtilTest, CopyDictionaryStringVectorAndFlatten)
{
    using omniruntime::mem::AlignedBuffer;
    using omniruntime::vec::DictionaryContainer;
    using omniruntime::vec::LargeStringContainer;
    using omniruntime::vec::NullsBuffer;
    using omniruntime::vec::Vector;

    constexpr int32_t dictionarySize = 5;
    auto* dictionaryValues = new Vector<LargeStringContainer<std::string_view>>(dictionarySize);
    std::vector<std::string> strings = {"zero", "one", "two", "three", "four"};
    for (int32_t i = 0; i < dictionarySize; ++i) {
        std::string_view value(strings[i]);
        dictionaryValues->SetValue(i, value);
    }

    constexpr int32_t rowCount = 6;
    int32_t ids[rowCount] = {4, 1, 3, 1, 0, 2};
    auto nullsBuffer = std::make_shared<AlignedBuffer<uint8_t>>(rowCount);
    auto* nulls = nullsBuffer->GetBuffer();
    for (int32_t i = 0; i < rowCount; ++i) {
        nulls[i] = 0;
    }

    auto dictionary = std::make_shared<DictionaryContainer<std::string_view>>(
        ids,
        rowCount,
        omniruntime::vec::unsafe::UnsafeStringVector::GetContainer(dictionaryValues),
        dictionaryValues->GetSize(),
        dictionaryValues->GetOffset());
    auto* input = new Vector<DictionaryContainer<std::string_view>>(
        rowCount, dictionary, new NullsBuffer(rowCount, nullsBuffer), false, omniruntime::type::OMNI_CHAR);
    input->SetNull(2);
    omnistream::VectorBatch vectorBatch(rowCount);
    vectorBatch.Append(input);

    std::vector<int32_t> positions = {3, 2, 5, 0};
    auto* resultBase = VectorBatchUtil::copyVectorByRowIds(&vectorBatch, 0, positions);
    auto* result = reinterpret_cast<Vector<LargeStringContainer<std::string_view>>*>(resultBase);

    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->GetSize(), static_cast<int32_t>(positions.size()));
    EXPECT_EQ(result->GetEncoding(), omniruntime::vec::OMNI_FLAT);
    EXPECT_EQ(result->GetTypeId(), omniruntime::type::OMNI_CHAR);
    EXPECT_EQ(result->GetValue(0), "one");
    EXPECT_TRUE(result->IsNull(1));
    EXPECT_EQ(result->GetValue(2), "two");
    EXPECT_EQ(result->GetValue(3), "four");

    vectorBatch.ClearVectors();
    delete result;
    delete input;
    delete dictionaryValues;
}

TEST(VectorBatchUtilTest, BuildNewVectorBatchByRowIds)
{
    constexpr int32_t rowCount = 6;
    omnistream::VectorBatch vectorBatch(rowCount);

    std::vector<int32_t> intValues = {10, 20, 30, 40, 50, 60};
    std::vector<int64_t> longValues = {100, 200, 300, 400, 500, 600};
    std::vector<int64_t> timestamps = {1001, 1002, 1003, 1004, 1005, 1006};
    std::vector<RowKind> rowKinds = {
        RowKind::INSERT,
        RowKind::DELETE,
        RowKind::UPDATE_AFTER,
        RowKind::INSERT,
        RowKind::UPDATE_BEFORE,
        RowKind::DELETE};

    auto* intVector = new omniruntime::vec::Vector<int32_t>(rowCount);
    auto* longVector = new omniruntime::vec::Vector<int64_t>(rowCount);
    for (int32_t row = 0; row < rowCount; ++row) {
        intVector->SetValue(row, intValues[row]);
        longVector->SetValue(row, longValues[row]);
        vectorBatch.setTimestamp(row, timestamps[row]);
        vectorBatch.setRowKind(row, rowKinds[row]);
    }
    intVector->SetNull(4);
    longVector->SetNull(1);
    vectorBatch.Append(intVector);
    vectorBatch.Append(longVector);

    std::vector<int32_t> rowIds = {5, 1, 4, 1, 0};
    auto* result = VectorBatchUtil::buildNewVectorBatchByRowIds(&vectorBatch, rowIds);

    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->GetRowCount(), static_cast<int32_t>(rowIds.size()));
    ASSERT_EQ(result->GetVectorCount(), 2);

    auto* resultInts = reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(result->Get(0));
    auto* resultLongs = reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(result->Get(1));
    for (int32_t row = 0; row < static_cast<int32_t>(rowIds.size()); ++row) {
        const auto sourceRow = rowIds[row];
        EXPECT_EQ(result->getTimestamp(row), timestamps[sourceRow]);
        EXPECT_EQ(result->getRowKind(row), rowKinds[sourceRow]);
        EXPECT_EQ(resultLongs->IsNull(row), longVector->IsNull(sourceRow));
        EXPECT_EQ(resultInts->IsNull(row), intVector->IsNull(sourceRow));
        if (!resultLongs->IsNull(row)) {
            EXPECT_EQ(resultLongs->GetValue(row), longValues[sourceRow]);
        }
        if (!resultInts->IsNull(row)) {
            EXPECT_EQ(resultInts->GetValue(row), intValues[sourceRow]);
        }
    }

    delete result->Get(0);
    delete result->Get(1);
    result->ClearVectors();
    delete result;

    vectorBatch.ClearVectors();
    delete intVector;
    delete longVector;
}

TEST(VectorBatchUtilTest, BuildNewVectorBatchByRowIdsRejectsNullBatch)
{
    std::vector<int32_t> rowIds = {0};
    EXPECT_THROW(VectorBatchUtil::buildNewVectorBatchByRowIds(nullptr, rowIds), std::runtime_error);
}

TEST(VectorBatchUtilTest, GetValueAtAsStrRendersVarcharAndChar)
{
    // A CSV partition key (VARCHAR or CHAR) is rendered through this helper when the
    // bucket path is built; OMNI_VARCHAR used to fall through to the default branch.
    constexpr int32_t rowCount = 3;
    omnistream::VectorBatch vectorBatch(rowCount);

    auto* varchar = new omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>(
        rowCount,
        std::make_shared<omniruntime::vec::LargeStringContainer<std::string_view>>(
            rowCount, omniruntime::vec::INITIAL_STRING_SIZE),
        nullptr,
        omniruntime::type::OMNI_VARCHAR);
    // LargeStringContainer 的向量没有 (size, capacity, typeId) 构造；短构造 (size) 会把
    // DataTypeId 硬设成 OMNI_CHAR，测不了 VARCHAR 分支，所以两个向量都走共享容器重载显式传 id。
    auto* charVector = new omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>(
        rowCount,
        std::make_shared<omniruntime::vec::LargeStringContainer<std::string_view>>(
            rowCount, omniruntime::vec::INITIAL_STRING_SIZE),
        nullptr,
        omniruntime::type::OMNI_CHAR);
    for (int32_t row = 0; row < rowCount; ++row) {
        std::string value = std::string(1, static_cast<char>('A' + row));
        // 该重载的 SetValue 取 std::string_view 的非 const 左值引用，不能直接传右值。
        std::string_view view(value);
        varchar->SetValue(row, view);
        charVector->SetValue(row, view);
    }
    varchar->SetNull(2);
    vectorBatch.Append(varchar);
    vectorBatch.Append(charVector);

    EXPECT_EQ(varchar->GetTypeId(), omniruntime::type::OMNI_VARCHAR);
    EXPECT_EQ(VectorBatchUtil::getValueAtAsStr(&vectorBatch, 0, 0), "A");
    EXPECT_EQ(VectorBatchUtil::getValueAtAsStr(&vectorBatch, 0, 1), "B");
    EXPECT_EQ(VectorBatchUtil::getValueAtAsStr(&vectorBatch, 0, 2), "NULL");
    EXPECT_EQ(VectorBatchUtil::getValueAtAsStr(&vectorBatch, 1, 0), "A");
    EXPECT_EQ(VectorBatchUtil::getValueAtAsStr(&vectorBatch, 1, 2), "C");

    vectorBatch.ClearVectors();
    delete varchar;
    delete charVector;
}

// CopyPositionsVector must keep the logical type of DECIMAL64 columns: the stock
// CopyPositions recreation degrades it to OMNI_LONG(2) on disconnected edges.
TEST(VectorBatchUtilTest, CopyPositionsVectorKeepsDecimal64TypeId)
{
    constexpr int32_t rowCount = 4;
    auto* decimalVector = new omniruntime::vec::Vector<int64_t>(rowCount, omniruntime::type::OMNI_DECIMAL64);
    const int64_t unscaled[] = {12345, -12345, 700, 5};
    for (int32_t row = 0; row < rowCount; ++row) {
        decimalVector->SetValue(row, unscaled[row]);
    }
    decimalVector->SetNull(3);

    int positions[] = {3, 0, 2};
    auto* copied = VectorBatchUtil::CopyPositionsVector(decimalVector, positions, 0, 3);

    ASSERT_NE(copied, nullptr);
    EXPECT_EQ(copied->GetTypeId(), omniruntime::type::OMNI_DECIMAL64);
    EXPECT_EQ(copied->GetSize(), 3);
    auto* copiedDecimal = reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(copied);
    EXPECT_TRUE(copiedDecimal->IsNull(0));
    EXPECT_EQ(copiedDecimal->GetValue(1), 12345);
    EXPECT_EQ(copiedDecimal->GetValue(2), 700);

    delete copied;
    delete decimalVector;
}

TEST(VectorBatchUtilTest, CopyPositionsVectorHonoursPositionOffset)
{
    // The function is always called with a non-zero offset when a batch is split
    // across channels, so the offset must be applied to the source positions.
    constexpr int32_t rowCount = 5;
    auto* decimalVector = new omniruntime::vec::Vector<int64_t>(rowCount, omniruntime::type::OMNI_DECIMAL64);
    for (int32_t row = 0; row < rowCount; ++row) {
        decimalVector->SetValue(row, (row + 1) * 100);
    }

    int positions[] = {-1, -1, 4, 2};
    auto* copied = VectorBatchUtil::CopyPositionsVector(decimalVector, positions, 2, 2);

    ASSERT_NE(copied, nullptr);
    EXPECT_EQ(copied->GetTypeId(), omniruntime::type::OMNI_DECIMAL64);
    auto* copiedDecimal = reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(copied);
    EXPECT_EQ(copiedDecimal->GetValue(0), 500);
    EXPECT_EQ(copiedDecimal->GetValue(1), 300);

    delete copied;
    delete decimalVector;
}

TEST(VectorBatchUtilTest, CopyPositionsVectorKeepsLongTypeIdUnchanged)
{
    // Control case: OMNI_LONG shares the same physical representation, so it must
    // keep going through the stock CopyPositions (and stay OMNI_LONG).
    constexpr int32_t rowCount = 3;
    auto* longVector = new omniruntime::vec::Vector<int64_t>(rowCount);
    for (int32_t row = 0; row < rowCount; ++row) {
        longVector->SetValue(row, row + 1);
    }

    int positions[] = {2, 0};
    auto* copied = VectorBatchUtil::CopyPositionsVector(longVector, positions, 0, 2);

    EXPECT_EQ(copied->GetTypeId(), omniruntime::type::OMNI_LONG);
    EXPECT_EQ(reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(copied)->GetValue(0), 3);
    EXPECT_EQ(reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(copied)->GetValue(1), 1);

    delete copied;
    delete longVector;
}

TEST(VectorBatchUtilTest, BuildNewVectorBatchByRowIdsKeepsDecimal64TypeId)
{
    // End-to-end version of the same defect: the per-channel split path used by
    // StreamPartitionerV2 must not lose the DECIMAL64 id.
    constexpr int32_t rowCount = 6;
    omnistream::VectorBatch vectorBatch(rowCount);

    auto* decimalVector = new omniruntime::vec::Vector<int64_t>(rowCount, omniruntime::type::OMNI_DECIMAL64);
    std::vector<int64_t> unscaled = {100, 200, 300, 400, 500, 600};
    std::vector<int64_t> timestamps = {1001, 1002, 1003, 1004, 1005, 1006};
    std::vector<RowKind> rowKinds = {
        RowKind::INSERT,
        RowKind::DELETE,
        RowKind::UPDATE_AFTER,
        RowKind::INSERT,
        RowKind::UPDATE_BEFORE,
        RowKind::DELETE};
    for (int32_t row = 0; row < rowCount; ++row) {
        decimalVector->SetValue(row, unscaled[row]);
        vectorBatch.setTimestamp(row, timestamps[row]);
        vectorBatch.setRowKind(row, rowKinds[row]);
    }
    decimalVector->SetNull(1);
    vectorBatch.Append(decimalVector);

    std::vector<int32_t> rowIds = {5, 1, 4, 0};
    auto* result = VectorBatchUtil::buildNewVectorBatchByRowIds(&vectorBatch, rowIds);

    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->GetRowCount(), static_cast<int32_t>(rowIds.size()));
    ASSERT_EQ(result->GetVectorCount(), 1);
    auto* resultDecimal = reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(result->Get(0));
    EXPECT_EQ(resultDecimal->GetTypeId(), omniruntime::type::OMNI_DECIMAL64);
    for (int32_t row = 0; row < static_cast<int32_t>(rowIds.size()); ++row) {
        const auto sourceRow = rowIds[row];
        EXPECT_EQ(result->getTimestamp(row), timestamps[sourceRow]);
        EXPECT_EQ(result->getRowKind(row), rowKinds[sourceRow]);
        EXPECT_EQ(resultDecimal->IsNull(row), decimalVector->IsNull(sourceRow));
        if (!resultDecimal->IsNull(row)) {
            EXPECT_EQ(resultDecimal->GetValue(row), unscaled[sourceRow]);
        }
    }

    delete result->Get(0);
    result->ClearVectors();
    delete result;

    vectorBatch.ClearVectors();
    delete decimalVector;
}
