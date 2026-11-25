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

////
//// Created by xxx on 2025/2/13.
////
//
////#include "core/operators/source/reader/KafkaSource.h"
////#include "core/operators/source/deserialize/SimpleStringSchema.h"
////#include "basictypes/utils/Collector.h"
//
//
// #include "../../../../core/operators/source/reader/KafkaSource.h"
// #include "../../../../core/operators/source/deserialize/SimpleStringSchema.h"
//
// #include <gtest/gtest.h>
//
// class TestCollector : public Collector {
// private:
//    StringView collectedRecord; // 新增变量用于保存 record 内容
// public:
//    void collect(Object * record) override {
//        auto* strPtr = reinterpret_cast<StringView*>(record);
//        if (strPtr) {
//            // 解引用指针获取 std::string 对象并输出
//            std::cout << "Collected string: " << strPtr->getData() << std::endl;
//            collectedRecord = *strPtr; // 保存 record 内容
//        } else {
//            std::cout << "Received a null pointer." << std::endl;
//            // no need to clear
//        }
//    }
//    void close() override {
//        // 关闭操作的具体逻辑
//    }
//    StringView &getCollectedRecord() {
//        return collectedRecord; // 提供获取保存内容的方法
//    }
//};
//
// TEST(DeserializationTest, SimpleStringTest) {
//    auto valueDeserializationSchema = std::make_shared<SimpleStringSchema>();
//    // 调用 valueOnly 方法创建 KafkaRecordDeserializationSchema<std::string> 对象
//    auto kafkaRecordDeserializationSchema = KafkaRecordDeserializationSchema::valueOnly(valueDeserializationSchema);
//    const uint8_t data[] = {65, 65, 65, 66, 67};
//    record.value = data;
//    record.value_length = 5;
//    std::string expected = "aaabc";
//    Object* ptr = valueDeserializationSchema->deserialize(data, 5);
//    auto* strPtr = static_cast<StringView*>(ptr);
//    EXPECT_EQ(strPtr->toString(), expected);
//    TestCollector collector;
//    kafkaRecordDeserializationSchema->deserialize(record, collector);
//    EXPECT_EQ(collector.getCollectedRecord().toString(), expected);
//}
