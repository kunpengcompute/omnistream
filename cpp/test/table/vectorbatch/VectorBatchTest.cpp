#include "table/data/vectorbatch/VectorBatch.h"
#include <gtest/gtest.h>
#include <memory>
#include <string.h>
#include <utility>
#include "OmniOperatorJIT/core/test/util/test_util.h"
#include "table/utils/VectorBatchSerializationUtils.h"
#include "table/utils/VectorBatchDeserializationUtils.h"

using namespace omnistream;

TEST(VectorBatchTest, TakesOwnershipOfBaseVectorBatch)
{
    constexpr int rowCount = 2;
    int32_t values[rowCount] = {10, 20};
    auto baseBatch = std::make_unique<omniruntime::vec::VectorBatch>(rowCount);
    baseBatch->Append(omniruntime::TestUtil::CreateVector<int32_t>(rowCount, values));

    omnistream::VectorBatch vectorBatch(std::move(baseBatch), nullptr, nullptr);

    EXPECT_EQ(vectorBatch.GetVectorCount(), 1);
    EXPECT_EQ(vectorBatch.GetValueAt<int32_t>(0, 0), 10);
    EXPECT_EQ(vectorBatch.GetValueAt<int32_t>(0, 1), 20);
}

TEST(VectorBatchTest, SetGetTimestampTest)
{
    omnistream::VectorBatch vectorBatch(2);
    vectorBatch.setTimestamp(0, 123);
    vectorBatch.setTimestamp(1, 456);
    EXPECT_EQ(vectorBatch.getTimestamp(0), 123);
    EXPECT_EQ(vectorBatch.getTimestamp(1), 456);
    vectorBatch.setTimestamp(1, 123);
    EXPECT_EQ(vectorBatch.getTimestamp(0), 123);
    EXPECT_EQ(vectorBatch.getTimestamp(1), 123);
}

TEST(VectorBatchTest, GetTimestampArrayTest)
{
    omnistream::VectorBatch vectorBatch(2);
    vectorBatch.setTimestamp(0, 123);
    vectorBatch.setTimestamp(1, 456);
    int64_t* timestamps = vectorBatch.getTimestamps();
    EXPECT_EQ(timestamps[0], 123);
    EXPECT_EQ(timestamps[1], 456);
}

TEST(VectorBatchTest, SetGetRowKindTest)
{
    omnistream::VectorBatch vectorBatch(2);
    RowKind rowKind1 = RowKind::DELETE;
    RowKind rowKind2 = RowKind::INSERT;
    vectorBatch.setRowKind(0, rowKind1);
    vectorBatch.setRowKind(1, rowKind2);
    EXPECT_EQ(vectorBatch.getRowKind(0), rowKind1);
    EXPECT_EQ(vectorBatch.getRowKind(1), rowKind2);
    vectorBatch.setRowKind(1, rowKind1);
    EXPECT_EQ(vectorBatch.getRowKind(0), rowKind1);
    EXPECT_EQ(vectorBatch.getRowKind(1), rowKind1);
}

TEST(VectorBatchTest, GetRowKindArrayTest)
{
    omnistream::VectorBatch vectorBatch(2);
    RowKind rowKind1 = RowKind::DELETE;
    RowKind rowKind2 = RowKind::INSERT;
    vectorBatch.setRowKind(0, rowKind1);
    vectorBatch.setRowKind(1, rowKind2);
    RowKind* rowKinds = vectorBatch.getRowKinds();
    EXPECT_EQ(rowKinds[0], rowKind1);
    EXPECT_EQ(rowKinds[1], rowKind2);
}

TEST(VectorBatchTest, DISABLED_NullValueTest)
{
    int rowCnt = 5;
    std::vector<int> col0 = {1, 2, 3, 4, 5};
    std::vector<long> col1 = {1000, 1011, 1001, 1000, 1000};
    std::vector<std::string> col2 = {"Facebook", "Google", "Baidu", "channel-7182", "Apple"};
    auto vb = new omnistream::VectorBatch(rowCnt);
    auto vec0 = omniruntime::TestUtil::CreateVector<int32_t>(rowCnt, col0.data());
    auto vec1 = omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col1.data());
    auto vec2 = omniruntime::TestUtil::CreateVarcharVector(col2.data(), rowCnt);
    /*set null
        1       1000    NULL
        2       1011    Google
        NULL    1001    Baidu
        NULL    1000    channel-7182
        5       NULL    Apple
    */
    vec0->SetNull(2);
    vec0->SetNull(3);
    vec1->SetNull(4);
    vec2->SetNull(0);
    vb->Append(vec0);
    vb->Append(vec1);
    vb->Append(vec2);
    std::string fileNme = "/tmp/null_output.txt";
    vb->writeToFile(fileNme);
}

int readDataType(uint8_t*& buffer)
{
    int8_t dataType;
    memcpy_s(&dataType, sizeof(dataType), buffer, sizeof(dataType));
    buffer += sizeof(dataType);
    return dataType;
}

TEST(VectorBatchTest, VectorBatchSerializationTestInt64)
{
    std::cout << "start task .........." << std::endl;

    int rowCnt = 2;
    std::vector<long> col0(rowCnt);
    std::vector<long> col1(rowCnt);
    // std::vector<long> col2(rowCnt);

    for (int i = 0; i < rowCnt; i++) {
        col0[i] = 0;
        col1[i] = i;
        // col2[i] = 2 * i;
    }

    omnistream::VectorBatch vectorBatch(2);
    RowKind rowKind1 = RowKind::DELETE;
    RowKind rowKind2 = RowKind::INSERT;
    vectorBatch.setRowKind(0, rowKind1);
    vectorBatch.setRowKind(1, rowKind2);

    vectorBatch.setTimestamp(0, 123);
    vectorBatch.setTimestamp(1, 456);

    // RowKind *rowKinds = vectorBatch.getRowKinds();
    omniruntime::vec::BaseVector* vec1 = omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col0.data());
    omniruntime::vec::BaseVector* vec2 = omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col1.data());

    vectorBatch.Append(vec1);
    vectorBatch.Append(vec2);

    std::cout << "for serialization, input vectorbatch created" << std::endl;

    int32_t batchSize = VectorBatchSerializationUtils::calculateVectorBatchSerializableSize(&vectorBatch);
    uint8_t* buffer = new uint8_t[batchSize];
    uint8_t* originalBuffer = buffer;

    SerializedBatchInfo serializedBatchInfo =
        VectorBatchSerializationUtils::serializeVectorBatch(&vectorBatch, batchSize, buffer);
    readDataType(serializedBatchInfo.dataAddress);

    omnistream::VectorBatch* deserializedVectorBatch =
        VectorBatchDeserializationUtils::deserializeVectorBatch(serializedBatchInfo.dataAddress);

    RowKind* rowKinds = deserializedVectorBatch->getRowKinds();
    EXPECT_EQ(rowKinds[0], rowKind1);
    EXPECT_EQ(rowKinds[1], rowKind2);

    int64_t* timestamps = deserializedVectorBatch->getTimestamps();
    EXPECT_EQ(timestamps[0], 123);
    EXPECT_EQ(timestamps[1], 456);

    for (int i = 0; i < rowCnt; i++) {
        EXPECT_EQ(
            reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(deserializedVectorBatch->Get(0))->GetValue(i), 0);
        EXPECT_EQ(
            reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(deserializedVectorBatch->Get(1))->GetValue(i), i);
    }
    vectorBatch.ClearVectors();
    deserializedVectorBatch->FreeAllVectors();
    delete[] originalBuffer;
    delete deserializedVectorBatch;
    delete vec1;
    delete vec2;
    std::cout << "end task .........." << std::endl;
}

TEST(VectorBatchTest, VectorBatchSerializationTestInt32)
{
    std::cout << "start task .........." << std::endl;

    int rowCnt = 2;
    std::vector<int32_t> col0(rowCnt);
    std::vector<int32_t> col1(rowCnt);
    // std::vector<long> col2(rowCnt);

    for (int i = 0; i < rowCnt; i++) {
        col0[i] = 0;
        col1[i] = i;
        // col2[i] = 2 * i;
    }

    omnistream::VectorBatch vectorBatch(2);
    RowKind rowKind1 = RowKind::DELETE;
    RowKind rowKind2 = RowKind::INSERT;
    vectorBatch.setRowKind(0, rowKind1);
    vectorBatch.setRowKind(1, rowKind2);

    vectorBatch.setTimestamp(0, 123);
    vectorBatch.setTimestamp(1, 456);

    // RowKind *rowKinds = vectorBatch.getRowKinds();

    omniruntime::vec::BaseVector* vec1 = omniruntime::TestUtil::CreateVector<int32_t>(rowCnt, col0.data());
    omniruntime::vec::BaseVector* vec2 = omniruntime::TestUtil::CreateVector<int32_t>(rowCnt, col1.data());
    vectorBatch.Append(vec1);
    vectorBatch.Append(vec2);

    std::cout << "for serialization, input vectorbatch created" << std::endl;

    int32_t batchSize = VectorBatchSerializationUtils::calculateVectorBatchSerializableSize(&vectorBatch);
    uint8_t* buffer = new uint8_t[batchSize];

    SerializedBatchInfo serializedBatchInfo =
        VectorBatchSerializationUtils::serializeVectorBatch(&vectorBatch, batchSize, buffer);
    uint8_t* serializeResult = serializedBatchInfo.dataAddress;
    readDataType(serializedBatchInfo.dataAddress);
    omnistream::VectorBatch* deserializedVectorBatch =
        VectorBatchDeserializationUtils::deserializeVectorBatch(serializedBatchInfo.dataAddress);

    RowKind* rowKinds = deserializedVectorBatch->getRowKinds();
    EXPECT_EQ(rowKinds[0], rowKind1);
    EXPECT_EQ(rowKinds[1], rowKind2);

    int64_t* timestamps = deserializedVectorBatch->getTimestamps();
    EXPECT_EQ(timestamps[0], 123);
    EXPECT_EQ(timestamps[1], 456);

    for (int i = 0; i < rowCnt; i++) {
        EXPECT_EQ(
            reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(deserializedVectorBatch->Get(0))->GetValue(i), 0);
        EXPECT_EQ(
            reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(deserializedVectorBatch->Get(1))->GetValue(i), i);
    }

    vectorBatch.ClearVectors();
    deserializedVectorBatch->FreeAllVectors();

    delete[] serializeResult;
    delete deserializedVectorBatch;
    delete vec1;
    delete vec2;
    std::cout << "end task .........." << std::endl;
}

TEST(VectorBatchTest, VectorBatchSerializationTestInt16)
{
    std::cout << "start task .........." << std::endl;

    int rowCnt = 2;
    std::vector<int16_t> col0(rowCnt);
    std::vector<int16_t> col1(rowCnt);
    // std::vector<long> col2(rowCnt);

    for (int i = 0; i < rowCnt; i++) {
        col0[i] = 0;
        col1[i] = i;
        // col2[i] = 2 * i;
    }

    omnistream::VectorBatch vectorBatch(2);
    RowKind rowKind1 = RowKind::DELETE;
    RowKind rowKind2 = RowKind::INSERT;
    vectorBatch.setRowKind(0, rowKind1);
    vectorBatch.setRowKind(1, rowKind2);

    vectorBatch.setTimestamp(0, 123);
    vectorBatch.setTimestamp(1, 456);

    // RowKind *rowKinds = vectorBatch.getRowKinds();

    omniruntime::vec::BaseVector* vec1 = omniruntime::TestUtil::CreateVector<int16_t>(rowCnt, col0.data());
    omniruntime::vec::BaseVector* vec2 = omniruntime::TestUtil::CreateVector<int16_t>(rowCnt, col1.data());
    vectorBatch.Append(vec1);
    vectorBatch.Append(vec2);

    std::cout << "for serialization, input vectorbatch created" << std::endl;

    int32_t batchSize = VectorBatchSerializationUtils::calculateVectorBatchSerializableSize(&vectorBatch);
    uint8_t* buffer = new uint8_t[batchSize];
    SerializedBatchInfo serializedBatchInfo =
        VectorBatchSerializationUtils::serializeVectorBatch(&vectorBatch, batchSize, buffer);
    uint8_t* serializeResult = serializedBatchInfo.dataAddress;
    readDataType(serializedBatchInfo.dataAddress);
    omnistream::VectorBatch* deserializedVectorBatch =
        VectorBatchDeserializationUtils::deserializeVectorBatch(serializedBatchInfo.dataAddress);

    RowKind* rowKinds = deserializedVectorBatch->getRowKinds();
    EXPECT_EQ(rowKinds[0], rowKind1);
    EXPECT_EQ(rowKinds[1], rowKind2);

    int64_t* timestamps = deserializedVectorBatch->getTimestamps();
    EXPECT_EQ(timestamps[0], 123);
    EXPECT_EQ(timestamps[1], 456);

    for (int i = 0; i < rowCnt; i++) {
        EXPECT_EQ(
            reinterpret_cast<omniruntime::vec::Vector<int16_t>*>(deserializedVectorBatch->Get(0))->GetValue(i), 0);
        EXPECT_EQ(
            reinterpret_cast<omniruntime::vec::Vector<int16_t>*>(deserializedVectorBatch->Get(1))->GetValue(i), i);
    }

    vectorBatch.ClearVectors();
    deserializedVectorBatch->FreeAllVectors();
    delete[] serializeResult;
    delete deserializedVectorBatch;
    delete vec1;
    delete vec2;
    std::cout << "end task .........." << std::endl;
}

TEST(VectorBatchTest, VectorBatchSerializationTestDouble)
{
    std::cout << "start task .........." << std::endl;

    int rowCnt = 2;
    std::vector<double> col0(rowCnt);
    std::vector<double> col1(rowCnt);
    // std::vector<long> col2(rowCnt);

    for (int i = 0; i < rowCnt; i++) {
        col0[i] = 0;
        col1[i] = i;
        // col2[i] = 2 * i;
    }

    omnistream::VectorBatch vectorBatch(2);
    RowKind rowKind1 = RowKind::DELETE;
    RowKind rowKind2 = RowKind::INSERT;
    vectorBatch.setRowKind(0, rowKind1);
    vectorBatch.setRowKind(1, rowKind2);

    vectorBatch.setTimestamp(0, 123);
    vectorBatch.setTimestamp(1, 456);

    // RowKind *rowKinds = vectorBatch.getRowKinds();

    omniruntime::vec::BaseVector* vec1 = omniruntime::TestUtil::CreateVector<double>(rowCnt, col0.data());
    omniruntime::vec::BaseVector* vec2 = omniruntime::TestUtil::CreateVector<double>(rowCnt, col1.data());
    vectorBatch.Append(vec1);
    vectorBatch.Append(vec2);
    std::cout << "for serialization, input vectorbatch created" << std::endl;
    int32_t batchSize = VectorBatchSerializationUtils::calculateVectorBatchSerializableSize(&vectorBatch);
    uint8_t* buffer = new uint8_t[batchSize];

    SerializedBatchInfo serializedBatchInfo =
        VectorBatchSerializationUtils::serializeVectorBatch(&vectorBatch, batchSize, buffer);
    uint8_t* serializeResult = serializedBatchInfo.dataAddress;
    readDataType(serializedBatchInfo.dataAddress);
    omnistream::VectorBatch* deserializedVectorBatch =
        VectorBatchDeserializationUtils::deserializeVectorBatch(serializedBatchInfo.dataAddress);

    RowKind* rowKinds = deserializedVectorBatch->getRowKinds();
    EXPECT_EQ(rowKinds[0], rowKind1);
    EXPECT_EQ(rowKinds[1], rowKind2);

    int64_t* timestamps = deserializedVectorBatch->getTimestamps();
    EXPECT_EQ(timestamps[0], 123);
    EXPECT_EQ(timestamps[1], 456);

    for (int i = 0; i < rowCnt; i++) {
        EXPECT_EQ(reinterpret_cast<omniruntime::vec::Vector<double>*>(deserializedVectorBatch->Get(0))->GetValue(i), 0);
        EXPECT_EQ(reinterpret_cast<omniruntime::vec::Vector<double>*>(deserializedVectorBatch->Get(1))->GetValue(i), i);
    }

    vectorBatch.ClearVectors();
    deserializedVectorBatch->FreeAllVectors();
    delete[] serializeResult;
    delete deserializedVectorBatch;
    delete vec1;
    delete vec2;
    std::cout << "end task .........." << std::endl;
}

TEST(VectorBatchTest, VectorBatchSerializationTestBoolean)
{
    std::cout << "start task .........." << std::endl;

    int rowCnt = 2;
    bool* col0 = new bool[rowCnt];
    bool* col1 = new bool[rowCnt];

    for (int i = 0; i < rowCnt; i++) {
        col0[i] = true;
        col1[i] = false;
    }

    omnistream::VectorBatch vectorBatch(2);
    RowKind rowKind1 = RowKind::DELETE;
    RowKind rowKind2 = RowKind::INSERT;
    vectorBatch.setRowKind(0, rowKind1);
    vectorBatch.setRowKind(1, rowKind2);

    vectorBatch.setTimestamp(0, 123);
    vectorBatch.setTimestamp(1, 456);

    // RowKind *rowKinds = vectorBatch.getRowKinds();

    omniruntime::vec::BaseVector* vec1 = omniruntime::TestUtil::CreateVector<bool>(rowCnt, col0);
    omniruntime::vec::BaseVector* vec2 = omniruntime::TestUtil::CreateVector<bool>(rowCnt, col1);
    vectorBatch.Append(vec1);
    vectorBatch.Append(vec2);

    std::cout << "for serialization, input vectorbatch created" << std::endl;

    int32_t batchSize = VectorBatchSerializationUtils::calculateVectorBatchSerializableSize(&vectorBatch);
    uint8_t* buffer = new uint8_t[batchSize];

    SerializedBatchInfo serializedBatchInfo =
        VectorBatchSerializationUtils::serializeVectorBatch(&vectorBatch, batchSize, buffer);
    uint8_t* serializeResult = serializedBatchInfo.dataAddress;
    readDataType(serializedBatchInfo.dataAddress);
    omnistream::VectorBatch* deserializedVectorBatch =
        VectorBatchDeserializationUtils::deserializeVectorBatch(serializedBatchInfo.dataAddress);

    RowKind* rowKinds = deserializedVectorBatch->getRowKinds();
    EXPECT_EQ(rowKinds[0], rowKind1);
    EXPECT_EQ(rowKinds[1], rowKind2);

    int64_t* timestamps = deserializedVectorBatch->getTimestamps();
    EXPECT_EQ(timestamps[0], 123);
    EXPECT_EQ(timestamps[1], 456);

    for (int i = 0; i < rowCnt; i++) {
        EXPECT_EQ(
            reinterpret_cast<omniruntime::vec::Vector<bool>*>(deserializedVectorBatch->Get(0))->GetValue(i), true);
        EXPECT_EQ(
            reinterpret_cast<omniruntime::vec::Vector<bool>*>(deserializedVectorBatch->Get(1))->GetValue(i), false);
    }

    vectorBatch.ClearVectors();
    deserializedVectorBatch->FreeAllVectors();
    delete[] serializeResult;
    delete deserializedVectorBatch;
    delete[] col0;
    delete[] col1;
    delete vec1;
    delete vec2;
    std::cout << "end task .........." << std::endl;
}

TEST(VectorBatchTest, DISABLED_VectorBatchSerializationTestString)
{
    std::cout << "start task .........." << std::endl;

    std::string valuePrefix = "hello_world__";

    int rowCnt = 2;

    Vector<LargeStringContainer<std::string_view>>* vec1 = new Vector<LargeStringContainer<std::string_view>>(rowCnt);
    Vector<LargeStringContainer<std::string_view>>* vec2 = new Vector<LargeStringContainer<std::string_view>>(rowCnt);

    for (int i = 0; i < rowCnt; i++) {
        std::string value = valuePrefix + std::to_string(0);
        std::string_view input(value.data(), value.size());
        vec1->SetValue(i, input);

        std::string value2 = valuePrefix + std::to_string(1);
        std::string_view input2(value2.data(), value2.size());
        vec2->SetValue(i, input2);
    }

    omnistream::VectorBatch vectorBatch(2);
    RowKind rowKind1 = RowKind::DELETE;
    RowKind rowKind2 = RowKind::INSERT;
    vectorBatch.setRowKind(0, rowKind1);
    vectorBatch.setRowKind(1, rowKind2);

    vectorBatch.setTimestamp(0, 123);
    vectorBatch.setTimestamp(1, 456);

    // RowKind *rowKinds = vectorBatch.getRowKinds();

    vectorBatch.Append(vec1);
    vectorBatch.Append(vec2);

    std::cout << "for serialization, input vectorbatch created" << std::endl;

    int32_t batchSize = VectorBatchSerializationUtils::calculateVectorBatchSerializableSize(&vectorBatch);
    uint8_t* buffer = new uint8_t[batchSize];

    SerializedBatchInfo serializedBatchInfo =
        VectorBatchSerializationUtils::serializeVectorBatch(&vectorBatch, batchSize, buffer);
    uint8_t* serializeResult = serializedBatchInfo.dataAddress;
    readDataType(serializedBatchInfo.dataAddress);
    omnistream::VectorBatch* deserializedVectorBatch =
        VectorBatchDeserializationUtils::deserializeVectorBatch(serializedBatchInfo.dataAddress);

    RowKind* rowKinds = deserializedVectorBatch->getRowKinds();
    EXPECT_EQ(rowKinds[0], rowKind1);
    EXPECT_EQ(rowKinds[1], rowKind2);

    int64_t* timestamps = deserializedVectorBatch->getTimestamps();
    EXPECT_EQ(timestamps[0], 123);
    EXPECT_EQ(timestamps[1], 456);

    for (int i = 0; i < rowCnt; i++) {
        EXPECT_EQ(
            reinterpret_cast<omniruntime::vec::Vector<LargeStringContainer<std::string_view>>*>(
                deserializedVectorBatch->Get(0))
                ->GetValue(i),
            "hello_world__0");
        EXPECT_EQ(
            reinterpret_cast<omniruntime::vec::Vector<LargeStringContainer<std::string_view>>*>(
                deserializedVectorBatch->Get(1))
                ->GetValue(i),
            "hello_world__1");
    }

    vectorBatch.ClearVectors();
    deserializedVectorBatch->FreeAllVectors();
    delete vec1;
    delete vec2;
    delete[] serializeResult;
    delete deserializedVectorBatch;
    std::cout << "end task .........." << std::endl;
}

TEST(VectorBatchTest, DISABLED_VectorBatchSerializationTestMix)
{
    std::cout << "start task .........." << std::endl;

    std::string value = "hello_world";
    std::string_view input(value.data(), value.size());

    int rowCnt = 2;

    Vector<LargeStringContainer<std::string_view>>* vec1 = new Vector<LargeStringContainer<std::string_view>>(rowCnt);
    Vector<int32_t>* vec2 = new Vector<int32_t>(rowCnt);
    Vector<double>* vec3 = new Vector<double>(rowCnt);
    Vector<bool>* vec4 = new Vector<bool>(rowCnt);

    for (int i = 0; i < rowCnt; i++) {
        vec1->SetValue(i, input);
        vec2->SetValue(i, 100);
        vec3->SetValue(i, 100.0);
        vec4->SetValue(i, true);
    }

    omnistream::VectorBatch vectorBatch(2);
    RowKind rowKind1 = RowKind::DELETE;
    RowKind rowKind2 = RowKind::INSERT;
    vectorBatch.setRowKind(0, rowKind1);
    vectorBatch.setRowKind(1, rowKind2);

    vectorBatch.setTimestamp(0, 123);
    vectorBatch.setTimestamp(1, 456);

    // RowKind *rowKinds = vectorBatch.getRowKinds();

    vectorBatch.Append(vec1);
    vectorBatch.Append(vec2);
    vectorBatch.Append(vec3);
    vectorBatch.Append(vec4);

    std::cout << "for serialization, input vectorbatch created" << std::endl;
    int32_t batchSize = VectorBatchSerializationUtils::calculateVectorBatchSerializableSize(&vectorBatch);
    uint8_t* buffer = new uint8_t[batchSize];

    SerializedBatchInfo serializedBatchInfo =
        VectorBatchSerializationUtils::serializeVectorBatch(&vectorBatch, batchSize, buffer);
    uint8_t* serializeResult = serializedBatchInfo.dataAddress;
    readDataType(serializedBatchInfo.dataAddress);
    omnistream::VectorBatch* deserializedVectorBatch =
        VectorBatchDeserializationUtils::deserializeVectorBatch(serializedBatchInfo.dataAddress);

    RowKind* rowKinds = deserializedVectorBatch->getRowKinds();
    EXPECT_EQ(rowKinds[0], rowKind1);
    EXPECT_EQ(rowKinds[1], rowKind2);

    int64_t* timestamps = deserializedVectorBatch->getTimestamps();
    EXPECT_EQ(timestamps[0], 123);
    EXPECT_EQ(timestamps[1], 456);

    for (int i = 0; i < rowCnt; i++) {
        EXPECT_EQ(
            reinterpret_cast<omniruntime::vec::Vector<LargeStringContainer<std::string_view>>*>(
                deserializedVectorBatch->Get(0))
                ->GetValue(i),
            "hello_world");
        EXPECT_EQ(
            reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(deserializedVectorBatch->Get(1))->GetValue(i), 100);
        EXPECT_EQ(
            reinterpret_cast<omniruntime::vec::Vector<double>*>(deserializedVectorBatch->Get(2))->GetValue(i), 100.0);
        EXPECT_EQ(
            reinterpret_cast<omniruntime::vec::Vector<bool>*>(deserializedVectorBatch->Get(3))->GetValue(i), true);
    }

    vectorBatch.ClearVectors();
    deserializedVectorBatch->FreeAllVectors();
    delete[] serializeResult;
    delete deserializedVectorBatch;
    delete vec1;
    delete vec2;
    delete vec3;
    delete vec4;

    std::cout << "end task .........." << std::endl;
}

TEST(VectorBatchTest, DISABLED_StringDictionaryContainer)
{
    std::cout << "start task .........." << std::endl;

    int dictSize = 10;

    Vector<LargeStringContainer<std::string_view>>* vec1 = new Vector<LargeStringContainer<std::string_view>>(dictSize);

    std::vector<std::string> persistentStrings(dictSize);
    for (int i = 0; i < dictSize; i++) {
        persistentStrings[i] = "hello_world" + std::to_string(i);
        std::string_view input(persistentStrings[i].data(), persistentStrings[i].size());
        vec1->SetValue(i, input);
    }

    int valueSize = 3;
    omnistream::VectorBatch vectorBatch(valueSize);

    for (int i = 0; i < valueSize; i++) {
        if (i % 2 == 0) {
            vectorBatch.setRowKind(i, RowKind::INSERT);
        } else {
            vectorBatch.setRowKind(i, RowKind::DELETE);
        }
    }

    for (int i = 0; i < valueSize; i++) {
        if (i % 2 == 0) {
            vectorBatch.setTimestamp(i, 123);
        } else {
            vectorBatch.setTimestamp(i, 456);
        }
    }

    int32_t values[] = {2, 3, 4};
    std::shared_ptr<AlignedBuffer<uint8_t>> nullsBuffer = std::make_shared<AlignedBuffer<uint8_t>>(valueSize);
    auto nulls = nullsBuffer->GetBuffer();
    for (int i = 0; i < valueSize; i++) {
        nulls[i] = vec1->IsNull(values[i]);
    }

    std::shared_ptr<DictionaryContainer<std::string_view>> dictionary =
        std::make_shared<DictionaryContainer<std::string_view>>(
            values, valueSize, unsafe::UnsafeStringVector::GetContainer(vec1), vec1->GetSize(), vec1->GetOffset());
    auto newNullsBuffer = new NullsBuffer(valueSize, nullsBuffer);
    Vector<DictionaryContainer<std::string_view>>* stringDictionaryVec =
        new Vector<DictionaryContainer<std::string_view>>(valueSize, dictionary, newNullsBuffer, false, OMNI_CHAR);
    Vector<DictionaryContainer<std::string_view>>* stringDictionaryVec2 =
        new Vector<DictionaryContainer<std::string_view>>(valueSize, dictionary, newNullsBuffer, false, OMNI_CHAR);

    vectorBatch.Append(stringDictionaryVec);
    // vectorBatch.Append(stringDictionaryVec2);

    int32_t batchSize = VectorBatchSerializationUtils::calculateVectorBatchSerializableSize(&vectorBatch);
    uint8_t* buffer = new uint8_t[batchSize];
    SerializedBatchInfo serializedBatchInfo =
        VectorBatchSerializationUtils::serializeVectorBatch(&vectorBatch, batchSize, buffer);
    uint8_t* serializeResult = serializedBatchInfo.dataAddress;
    readDataType(serializedBatchInfo.dataAddress);

    omnistream::VectorBatch* deserializedVectorBatch =
        VectorBatchDeserializationUtils::deserializeVectorBatch(serializedBatchInfo.dataAddress);

    RowKind* rowKinds = deserializedVectorBatch->getRowKinds();
    for (int i = 0; i < valueSize; i++) {
        if (i % 2 == 0) {
            EXPECT_EQ(rowKinds[i], RowKind::INSERT);
        } else {
            EXPECT_EQ(rowKinds[i], RowKind::DELETE);
        }
    }

    int64_t* timestamps = deserializedVectorBatch->getTimestamps();

    for (int i = 0; i < valueSize; i++) {
        if (i % 2 == 0) {
            EXPECT_EQ(timestamps[i], 123);
        } else {
            EXPECT_EQ(timestamps[i], 456);
        }
    }

    Vector<DictionaryContainer<std::string_view>>* desResult =
        reinterpret_cast<Vector<DictionaryContainer<std::string_view>>*>(deserializedVectorBatch->Get(0));

    std::shared_ptr<DictionaryContainer<std::string_view>> string_dictionary =
        static_cast<std::shared_ptr<DictionaryContainer<std::string_view>>>(
            unsafe::UnsafeDictionaryVector::GetDictionaryOriginal(desResult));
    std::shared_ptr<LargeStringContainer<std::string_view>> stringContainer =
        unsafe::UnsafeDictionaryContainer::GetStringDictionaryOriginal(string_dictionary.get());
    int32_t dictSizeParse = unsafe::UnsafeDictionaryContainer::GetDictSize(string_dictionary.get());

    EXPECT_EQ(desResult->GetSize(), valueSize);
    EXPECT_EQ(dictSize, dictSizeParse);

    for (int i = 0; i < dictSize; i++) {
        std::string value = "hello_world" + std::to_string(i);
        EXPECT_EQ(stringContainer->GetValue(i), value);
    }

    for (int i = 0; i < valueSize; i++) {
        std::string value = "hello_world" + std::to_string(values[i]);
        EXPECT_EQ(desResult->GetValue(i), value);
    }

    vectorBatch.ClearVectors();
    deserializedVectorBatch->FreeAllVectors();
    delete[] serializeResult;
    delete deserializedVectorBatch;
    delete stringDictionaryVec;
    delete stringDictionaryVec2;
    delete vec1;

    std::cout << "end task .........." << std::endl;
}

TEST(VectorBatchTest, DISABLED_StringDictionaryContainerAndOtherVector)
{
    std::cout << "start task .........." << std::endl;

    int dictSize = 10;

    Vector<LargeStringContainer<std::string_view>>* vec1 = new Vector<LargeStringContainer<std::string_view>>(dictSize);

    for (int i = 0; i < dictSize; i++) {
        std::string value = "hello_world" + std::to_string(i);
        std::string_view input(value.data(), value.size());
        vec1->SetValue(i, input);
    }

    int valueSize = 3;
    omnistream::VectorBatch vectorBatch(valueSize);

    RowKind rowKind1 = RowKind::DELETE;
    RowKind rowKind2 = RowKind::INSERT;

    for (int i = 0; i < valueSize; i++) {
        if (i % 2 == 0) {
            vectorBatch.setRowKind(i, RowKind::INSERT);
        } else {
            vectorBatch.setRowKind(i, RowKind::DELETE);
        }
    }

    for (int i = 0; i < valueSize; i++) {
        if (i % 2 == 0) {
            vectorBatch.setTimestamp(i, 123);
        } else {
            vectorBatch.setTimestamp(i, 456);
        }
    }

    // RowKind *rowKinds = vectorBatch.getRowKinds();

    // vectorBatch.Append(vec1);

    int32_t values[] = {2, 3, 4};
    std::shared_ptr<AlignedBuffer<uint8_t>> nullsBuffer = std::make_shared<AlignedBuffer<uint8_t>>(valueSize);
    auto newNullsBuffer = new NullsBuffer(valueSize, nullsBuffer);
    auto nulls = nullsBuffer->GetBuffer();
    for (int i = 0; i < valueSize; i++) {
        nulls[i] = vec1->IsNull(values[i]);
    }

    auto dictionary = std::make_shared<DictionaryContainer<std::string_view>>(
        values, valueSize, unsafe::UnsafeStringVector::GetContainer(vec1), vec1->GetSize(), vec1->GetOffset());

    auto stringDictionaryVec =
        new Vector<DictionaryContainer<std::string_view>>(valueSize, dictionary, newNullsBuffer, false, OMNI_CHAR);

    vectorBatch.Append(stringDictionaryVec);

    Vector<int32_t>* vec2 = new Vector<int32_t>(valueSize);
    Vector<double>* vec3 = new Vector<double>(valueSize);
    Vector<bool>* vec4 = new Vector<bool>(valueSize);

    for (int i = 0; i < valueSize; i++) {
        vec2->SetValue(i, 100);
        vec3->SetValue(i, 100.0);
        vec4->SetValue(i, true);
    }

    vectorBatch.Append(vec2);
    vectorBatch.Append(vec3);
    vectorBatch.Append(vec4);

    int32_t batchSize = VectorBatchSerializationUtils::calculateVectorBatchSerializableSize(&vectorBatch);
    uint8_t* buffer = new uint8_t[batchSize];

    SerializedBatchInfo serializedBatchInfo =
        VectorBatchSerializationUtils::serializeVectorBatch(&vectorBatch, batchSize, buffer);
    uint8_t* serializeResult = serializedBatchInfo.dataAddress;
    readDataType(serializedBatchInfo.dataAddress);
    omnistream::VectorBatch* deserializedVectorBatch =
        VectorBatchDeserializationUtils::deserializeVectorBatch(serializedBatchInfo.dataAddress);

    RowKind* rowKinds = deserializedVectorBatch->getRowKinds();
    for (int i = 0; i < valueSize; i++) {
        if (i % 2 == 0) {
            EXPECT_EQ(rowKinds[i], RowKind::INSERT);
        } else {
            EXPECT_EQ(rowKinds[i], RowKind::DELETE);
        }
    }

    int64_t* timestamps = deserializedVectorBatch->getTimestamps();

    for (int i = 0; i < valueSize; i++) {
        if (i % 2 == 0) {
            EXPECT_EQ(timestamps[i], 123);
        } else {
            EXPECT_EQ(timestamps[i], 456);
        }
    }

    Vector<DictionaryContainer<std::string_view>>* desResult =
        reinterpret_cast<Vector<DictionaryContainer<std::string_view>>*>(deserializedVectorBatch->Get(0));

    std::shared_ptr<DictionaryContainer<std::string_view>> string_dictionary =
        static_cast<std::shared_ptr<DictionaryContainer<std::string_view>>>(
            unsafe::UnsafeDictionaryVector::GetDictionaryOriginal(desResult));
    std::shared_ptr<LargeStringContainer<std::string_view>> stringContainer =
        unsafe::UnsafeDictionaryContainer::GetStringDictionaryOriginal(string_dictionary.get());
    int32_t dictSizeParse = unsafe::UnsafeDictionaryContainer::GetDictSize(string_dictionary.get());

    EXPECT_EQ(desResult->GetSize(), valueSize);
    EXPECT_EQ(dictSize, dictSizeParse);

    for (int i = 0; i < dictSize; i++) {
        std::string value = "hello_world" + std::to_string(i);
        EXPECT_EQ(stringContainer->GetValue(i), value);
    }

    for (int i = 0; i < valueSize; i++) {
        std::string value = "hello_world" + std::to_string(values[i]);
        EXPECT_EQ(desResult->GetValue(i), value);
    }

    for (int i = 0; i < valueSize; i++) {
        EXPECT_EQ(
            reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(deserializedVectorBatch->Get(1))->GetValue(i), 100);
        EXPECT_EQ(
            reinterpret_cast<omniruntime::vec::Vector<double>*>(deserializedVectorBatch->Get(2))->GetValue(i), 100.0);
        EXPECT_EQ(
            reinterpret_cast<omniruntime::vec::Vector<bool>*>(deserializedVectorBatch->Get(3))->GetValue(i), true);
    }
    std::cout << "end task .........." << std::endl;

    vectorBatch.ClearVectors();
    deserializedVectorBatch->FreeAllVectors();
    delete[] serializeResult;
    delete deserializedVectorBatch;
    delete stringDictionaryVec;
    delete vec1;
    delete vec2;
    delete vec3;
    delete vec4;
}

// VectorBatch::copy() must keep DECIMAL64 columns as OMNI_DECIMAL64: the stock
// path degrades them to OMNI_LONG(2), while DECIMAL128 keeps its id.
TEST(VectorBatchTest, CopyKeepsDecimal64LogicalType)
{
    constexpr int32_t rowCount = 3;
    omnistream::VectorBatch vectorBatch(rowCount);

    auto* decimalVector = new omniruntime::vec::Vector<int64_t>(rowCount, omniruntime::type::OMNI_DECIMAL64);
    const int64_t unscaled[] = {12345, -700, 5};
    for (int32_t row = 0; row < rowCount; ++row) {
        decimalVector->SetValue(row, unscaled[row]);
        vectorBatch.setTimestamp(row, 1000 + row);
        vectorBatch.setRowKind(row, row == 1 ? RowKind::DELETE : RowKind::INSERT);
    }
    decimalVector->SetNull(2);
    vectorBatch.Append(decimalVector);

    omnistream::VectorBatch* copied = vectorBatch.copy();
    ASSERT_NE(copied, nullptr);
    ASSERT_EQ(copied->GetVectorCount(), 1);
    EXPECT_EQ(copied->Get(0)->GetTypeId(), omniruntime::type::OMNI_DECIMAL64);

    auto* copiedDecimal = reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(copied->Get(0));
    EXPECT_EQ(copiedDecimal->GetValue(0), 12345);
    EXPECT_EQ(copiedDecimal->GetValue(1), -700);
    EXPECT_TRUE(copiedDecimal->IsNull(2));
    EXPECT_EQ(copied->getTimestamp(0), 1000);
    EXPECT_EQ(copied->getTimestamp(1), 1001);
    EXPECT_EQ(copied->getRowKind(0), RowKind::INSERT);
    EXPECT_EQ(copied->getRowKind(1), RowKind::DELETE);

    copied->FreeAllVectors();
    delete copied;
    vectorBatch.ClearVectors();
    delete decimalVector;
}

TEST(VectorBatchTest, CopyKeepsDecimal128LogicalType)
{
    // Control case: DECIMAL128 was already correct, pin it against regressions.
    constexpr int32_t rowCount = 2;
    omnistream::VectorBatch vectorBatch(rowCount);
    auto* decimalVector = new omniruntime::vec::Vector<omniruntime::type::Decimal128>(rowCount);
    decimalVector->SetValue(0, omniruntime::type::Decimal128(omniruntime::type::int128_t(908)));
    decimalVector->SetValue(1, omniruntime::type::Decimal128(omniruntime::type::int128_t(-1)));
    vectorBatch.Append(decimalVector);

    omnistream::VectorBatch* copied = vectorBatch.copy();
    ASSERT_NE(copied, nullptr);
    EXPECT_EQ(copied->Get(0)->GetTypeId(), omniruntime::type::OMNI_DECIMAL128);
    auto* copiedDecimal = reinterpret_cast<omniruntime::vec::Vector<omniruntime::type::Decimal128>*>(copied->Get(0));
    EXPECT_EQ(copiedDecimal->GetValue(0).ToString(), "908");
    EXPECT_EQ(copiedDecimal->GetValue(1).ToString(), "-1");

    copied->FreeAllVectors();
    delete copied;
    vectorBatch.ClearVectors();
    delete decimalVector;
}

TEST(VectorBatchTest, CopyKeepsLongColumnAsLong)
{
    // Control case: a plain BIGINT column must not be turned into DECIMAL64.
    constexpr int32_t rowCount = 2;
    omnistream::VectorBatch vectorBatch(rowCount);
    auto* longVector = new omniruntime::vec::Vector<int64_t>(rowCount);
    longVector->SetValue(0, 11);
    longVector->SetValue(1, 22);
    vectorBatch.Append(longVector);

    omnistream::VectorBatch* copied = vectorBatch.copy();
    ASSERT_NE(copied, nullptr);
    EXPECT_EQ(copied->Get(0)->GetTypeId(), omniruntime::type::OMNI_LONG);
    EXPECT_EQ(reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(copied->Get(0))->GetValue(1), 22);

    copied->FreeAllVectors();
    delete copied;
    vectorBatch.ClearVectors();
    delete longVector;
}

// The wire format carries DataTypeId per column, so a DECIMAL64 column must come back
// as OMNI_DECIMAL64 (and not as OMNI_LONG) after the network hop.
TEST(VectorBatchTest, VectorBatchSerializationTestDecimal64)
{
    constexpr int32_t rowCnt = 3;
    omnistream::VectorBatch vectorBatch(rowCnt);
    vectorBatch.setRowKind(0, RowKind::INSERT);
    vectorBatch.setRowKind(1, RowKind::DELETE);
    vectorBatch.setRowKind(2, RowKind::UPDATE_AFTER);
    vectorBatch.setTimestamp(0, 123);
    vectorBatch.setTimestamp(1, 456);
    vectorBatch.setTimestamp(2, 789);

    auto* decimalVector = new omniruntime::vec::Vector<int64_t>(rowCnt, omniruntime::type::OMNI_DECIMAL64);
    decimalVector->SetValue(0, 12345);
    decimalVector->SetValue(1, -12345);
    decimalVector->SetValue(2, 0);
    decimalVector->SetNull(2);
    vectorBatch.Append(decimalVector);

    int32_t batchSize = VectorBatchSerializationUtils::calculateVectorBatchSerializableSize(&vectorBatch);
    uint8_t* buffer = new uint8_t[batchSize];
    uint8_t* originalBuffer = buffer;
    SerializedBatchInfo serializedBatchInfo =
        VectorBatchSerializationUtils::serializeVectorBatch(&vectorBatch, batchSize, buffer);
    readDataType(serializedBatchInfo.buffer);

    omnistream::VectorBatch* deserializedVectorBatch =
        VectorBatchDeserializationUtils::deserializeVectorBatch(serializedBatchInfo.buffer);

    ASSERT_NE(deserializedVectorBatch, nullptr);
    ASSERT_EQ(deserializedVectorBatch->GetVectorCount(), 1);
    EXPECT_EQ(deserializedVectorBatch->Get(0)->GetTypeId(), omniruntime::type::OMNI_DECIMAL64);

    auto* deserializedDecimal = reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(deserializedVectorBatch->Get(0));
    EXPECT_EQ(deserializedDecimal->GetValue(0), 12345);
    EXPECT_EQ(deserializedDecimal->GetValue(1), -12345);
    EXPECT_TRUE(deserializedDecimal->IsNull(2));
    EXPECT_EQ(deserializedVectorBatch->getRowKind(1), RowKind::DELETE);
    EXPECT_EQ(deserializedVectorBatch->getTimestamp(2), 789);

    vectorBatch.ClearVectors();
    deserializedVectorBatch->FreeAllVectors();
    delete[] originalBuffer;
    delete deserializedVectorBatch;
    delete decimalVector;
}

TEST(VectorBatchTest, VectorBatchSerializationTestDecimal128)
{
    constexpr int32_t rowCnt = 2;
    omnistream::VectorBatch vectorBatch(rowCnt);
    auto* decimalVector = new omniruntime::vec::Vector<omniruntime::type::Decimal128>(rowCnt);
    decimalVector->SetValue(0, omniruntime::type::Decimal128(omniruntime::type::int128_t(908)));
    decimalVector->SetValue(1, omniruntime::type::Decimal128(omniruntime::type::int128_t(-908)));
    vectorBatch.Append(decimalVector);

    int32_t batchSize = VectorBatchSerializationUtils::calculateVectorBatchSerializableSize(&vectorBatch);
    uint8_t* buffer = new uint8_t[batchSize];
    uint8_t* originalBuffer = buffer;
    SerializedBatchInfo serializedBatchInfo =
        VectorBatchSerializationUtils::serializeVectorBatch(&vectorBatch, batchSize, buffer);
    readDataType(serializedBatchInfo.buffer);

    omnistream::VectorBatch* deserializedVectorBatch =
        VectorBatchDeserializationUtils::deserializeVectorBatch(serializedBatchInfo.buffer);

    ASSERT_NE(deserializedVectorBatch, nullptr);
    EXPECT_EQ(deserializedVectorBatch->Get(0)->GetTypeId(), omniruntime::type::OMNI_DECIMAL128);
    auto* deserializedDecimal =
        reinterpret_cast<omniruntime::vec::Vector<omniruntime::type::Decimal128>*>(deserializedVectorBatch->Get(0));
    EXPECT_EQ(deserializedDecimal->GetValue(0).ToString(), "908");
    EXPECT_EQ(deserializedDecimal->GetValue(1).ToString(), "-908");

    vectorBatch.ClearVectors();
    deserializedVectorBatch->FreeAllVectors();
    delete[] originalBuffer;
    delete deserializedVectorBatch;
    delete decimalVector;
}

TEST(VectorBatchTest, VectorBatchSerializationTestLongStaysLong)
{
    // Control case: BIGINT must keep travelling as OMNI_LONG.
    constexpr int32_t rowCnt = 2;
    omnistream::VectorBatch vectorBatch(rowCnt);
    auto* longVector = new omniruntime::vec::Vector<int64_t>(rowCnt);
    longVector->SetValue(0, 11);
    longVector->SetValue(1, 22);
    vectorBatch.Append(longVector);

    int32_t batchSize = VectorBatchSerializationUtils::calculateVectorBatchSerializableSize(&vectorBatch);
    uint8_t* buffer = new uint8_t[batchSize];
    uint8_t* originalBuffer = buffer;
    SerializedBatchInfo serializedBatchInfo =
        VectorBatchSerializationUtils::serializeVectorBatch(&vectorBatch, batchSize, buffer);
    readDataType(serializedBatchInfo.buffer);

    omnistream::VectorBatch* deserializedVectorBatch =
        VectorBatchDeserializationUtils::deserializeVectorBatch(serializedBatchInfo.buffer);

    ASSERT_NE(deserializedVectorBatch, nullptr);
    EXPECT_EQ(deserializedVectorBatch->Get(0)->GetTypeId(), omniruntime::type::OMNI_LONG);
    EXPECT_EQ(reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(deserializedVectorBatch->Get(0))->GetValue(0), 11);

    vectorBatch.ClearVectors();
    deserializedVectorBatch->FreeAllVectors();
    delete[] originalBuffer;
    delete deserializedVectorBatch;
    delete longVector;
}
