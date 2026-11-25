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

#pragma once

#include <algorithm>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>
#include <nlohmann/json.hpp>

#include "common.h"
#include "OmniOperatorJIT/core/src/type/data_type.h"
#include "core/api/common/state/StateDescriptor.h"
#include "core/memory/DataInputDeserializer.h"
#include "core/typeinfo/BasicTypeInfo.h"
#include "core/typeutils/LongSerializer.h"
#include "core/typeutils/MapSerializer.h"
#include "core/typeutils/TupleSerializer.h"
#include "core/utils/ByteView.h"
#include "runtime/checkpoint/FlinkSavepointAdaptorInfo.h"
#include "runtime/state/RegisteredKeyValueStateBackendMetaInfo.h"
#include "runtime/state/metainfo/StateMetaInfoSnapshot.h"
#include "table/data/TimestampData.h"
#include "table/types/logical/LogicalType.h"
#include "table/types/logical/RowType.h"
#include "table/types/logical/TimestampWithLocalTimeZoneType.h"
#include "table/types/logical/TimestampWithoutTimeZoneType.h"
#include "table/typeutils/RowDataSerializer.h"

namespace omnistream {

// StreamingJoin SP 格式互通工具类保持 header-only，便于 adaptor/factory
// 在不同编译单元直接复用同一套判定、解析与元数据构造逻辑。
// 所有成员函数均为 inline，避免头文件实现被多个 .cpp 引入时产生重复定义。
class StreamingJoinSavepointUtil {
public:
    // Flink Join value 中已解析出的计数和关联数。
    struct ParsedJoinValue {
        // 当前 Join tuple 对应的匹配计数。
        int32_t count = 0;
        // left outer join 保留侧使用的关联记录数，inner join 时保持默认值。
        int32_t numAssociations = 0;
        // 标记该 value 是否采用 left outer join 的三字段布局。
        bool outerJoinState = false;
    };

    // StreamingJoin 左侧 Omni 主状态的逻辑名称。
    static constexpr const char* LEFT_STATE_NAME = "left-records";
    // StreamingJoin 右侧 Omni 主状态的逻辑名称。
    static constexpr const char* RIGHT_STATE_NAME = "right-records";
    // 算子描述中 inner join 的类型名称。
    static constexpr const char* INNER_JOIN_TYPE = "InnerJoin";
    // 算子描述中无唯一键输入规格的名称。
    static constexpr const char* NO_UNIQUE_KEY = "NoUniqueKey";
    // 算子描述中 left outer join 的类型名称。
    static constexpr const char* LEFT_OUTER_JOIN_TYPE = "LeftOuterJoin";

    // Join 类型在算子描述中的字段名。
    static constexpr const char* JOIN_TYPE_FIELD = "joinType";
    // 左侧输入规格在算子描述中的字段名。
    static constexpr const char* LEFT_INPUT_SPEC_FIELD = "leftInputSpec";
    // 右侧输入规格在算子描述中的字段名。
    static constexpr const char* RIGHT_INPUT_SPEC_FIELD = "rightInputSpec";
    // 左侧唯一键配置在算子描述中的字段名。
    static constexpr const char* LEFT_UNIQUE_KEYS_FIELD = "leftUniqueKeys";
    // 右侧唯一键配置在算子描述中的字段名。
    static constexpr const char* RIGHT_UNIQUE_KEYS_FIELD = "rightUniqueKeys";
    // 左侧状态名称在算子描述中的字段名。
    static constexpr const char* LEFT_STATE_NAME_FIELD = "leftStateName";
    // 右侧状态名称在算子描述中的字段名。
    static constexpr const char* RIGHT_STATE_NAME_FIELD = "rightStateName";
    // 左侧输入字段类型数组在算子描述中的字段名。
    static constexpr const char* LEFT_INPUT_TYPES_FIELD = "leftInputTypes";
    // 右侧输入字段类型数组在算子描述中的字段名。
    static constexpr const char* RIGHT_INPUT_TYPES_FIELD = "rightInputTypes";
    // 左侧 Join key 索引数组在算子描述中的字段名。
    static constexpr const char* LEFT_JOIN_KEY_FIELD = "leftJoinKey";
    // 右侧 Join key 索引数组在算子描述中的字段名。
    static constexpr const char* RIGHT_JOIN_KEY_FIELD = "rightJoinKey";
    // null 过滤配置在算子描述中的字段名。
    static constexpr const char* FILTER_NULLS_FIELD = "filterNulls";
    // XXH128 hash 序列化结果占用的字节数。
    static constexpr int XXH128_SERIALIZED_BYTES = 16;

    // WindowJoin 算子类型名称。
    static constexpr const char* WINDOW_JOIN_OPERATOR_TYPE = "WindowJoin";
    // WindowJoin 时区字段名。
    static constexpr const char* SHIFT_TIMEZONE_FIELD = "shiftTimeZone";
    // WindowJoin 窗口结束索引字段名。
    static constexpr const char* LEFT_WINDOW_END_INDEX_FIELD = "leftWindowEndIndex";
    static constexpr const char* RIGHT_WINDOW_END_INDEX_FIELD = "rightWindowEndIndex";

    // 工具类仅提供静态方法，不允许创建实例。
    StreamingJoinSavepointUtil() = delete;

    // 根据完整算子描述解析 StreamingJoin 兼容格式适配器类型。
    // 返回 FlinkSavepointAdaptorType::None 表示当前算子描述超出已支持范围。
    static FlinkSavepointAdaptorType getAdaptorType(const nlohmann::json& description);

    // 按固定校验顺序构造当前 StreamingJoin 不支持兼容格式互通的具体原因。
    static std::string buildUnsupportedReason(const nlohmann::json& description);

    // 根据完整算子描述解析 WindowJoin 兼容格式适配器类型。
    // 返回 FlinkSavepointAdaptorType::None 表示当前算子描述超出已支持范围。
    static FlinkSavepointAdaptorType getWindowJoinAdaptorType(const nlohmann::json& description);

    // 按固定校验顺序构造 WindowJoin 不支持兼容格式互通的具体原因。
    static std::string buildWindowJoinUnsupportedReason(const nlohmann::json& description);

    // 解析 Flink StreamingJoin MapState value，按 inner/left outer 状态布局读取计数字段。
    static ParsedJoinValue parseFlinkJoinValue(ByteView rawValue, bool outerJoinState);

    // 基于 Omni 源状态元数据创建 Flink StreamingJoin logical MapState 元数据。
    static std::shared_ptr<StateMetaInfoSnapshot> createFlinkMapStateSnapshot(
        const std::string& flinkStateName,
        const StateMetaInfoSnapshot& sourceMetaInfo,
        const std::vector<std::string>& inputTypeNames,
        bool outerJoinState);

private:
    // 防御性读取算子描述中的字符串字段。
    // 字段缺失或 JSON 类型不是字符串时统一返回空字符串，由上层生成明确的不支持原因。
    static std::string getStringField(const nlohmann::json& description, const std::string& fieldName);

    // 判断指定 inputSpec 字段是否明确声明为 NoUniqueKey。
    static bool isNoUniqueKeySpec(const nlohmann::json& description, const std::string& fieldName);

    // 判断指定 uniqueKeys 字段中是否包含任意有效唯一键定义。
    static bool hasUniqueKeys(const nlohmann::json& description, const std::string& fieldName);

    // 校验左右主状态名称是否缺省/为空，或符合 Flink StreamingJoin 默认约定名称。
    static bool areStateNamesEmptyOrDefault(const nlohmann::json& description);

    // 校验 Join key 索引合法，并确认对应输入字段均为 BIGINT 类型。
    static bool hasSupportedJoinKeys(
        const nlohmann::json& description, const std::string& keyField, const std::string& inputTypeField);

    // 判断单个 Flink 输入逻辑类型是否受当前 StreamingJoin adaptor 支持。
    static bool isSupportedInputType(const std::string& typeName);

    // 校验指定输入类型数组中的所有字段都位于 StreamingJoin 当前端到端支持范围内。
    static bool isAllOfSupportedInputTypes(const nlohmann::json& description, const std::string& inputTypeField);

    // 校验 filterNulls 字段存在，并且为布尔值或非空布尔数组。
    static bool hasFilterNullsContract(const nlohmann::json& description);

    // 校验 WindowJoin 算子的字段完整性与类型支持。
    // 返回空字符串表示全部校验通过，否则返回第一个失败的具体原因。
    static std::string validateWindowJoinFields(const nlohmann::json& description);

    // 校验 inner 和 left outer StreamingJoin 共同使用的 NoUniqueKey 元数据约束。
    // Join 类型映射保留在该方法之外，使调用方能够直接复用已经选定的 adaptorType。
    static bool hasSupportedNoUniqueKeyJoinContract(const nlohmann::json& description);
};

inline std::string StreamingJoinSavepointUtil::getStringField(
    const nlohmann::json& description, const std::string& fieldName)
{
    if (!description.contains(fieldName) || !description[fieldName].is_string()) {
        return "";
    }
    return description[fieldName].get<std::string>();
}

inline FlinkSavepointAdaptorType StreamingJoinSavepointUtil::getAdaptorType(const nlohmann::json& description)
{
    const std::string joinType = getStringField(description, JOIN_TYPE_FIELD);
    if (!hasSupportedNoUniqueKeyJoinContract(description)) {
        return FlinkSavepointAdaptorType::None;
    }
    if (joinType == INNER_JOIN_TYPE) {
        return FlinkSavepointAdaptorType::StreamingJoinNoUniqueKeyAdaptor;
    }
    if (joinType == LEFT_OUTER_JOIN_TYPE) {
        return FlinkSavepointAdaptorType::StreamingLeftOuterJoinNoUniqueKeyAdaptor;
    }
    return FlinkSavepointAdaptorType::None;
}

inline std::string StreamingJoinSavepointUtil::buildUnsupportedReason(const nlohmann::json& description)
{
    const std::string joinType = getStringField(description, JOIN_TYPE_FIELD);
    if (joinType != INNER_JOIN_TYPE && joinType != LEFT_OUTER_JOIN_TYPE) {
        return "StreamingJoin compatible savepoint only supports InnerJoin and LeftOuterJoin";
    }
    if (!isNoUniqueKeySpec(description, LEFT_INPUT_SPEC_FIELD) ||
        !isNoUniqueKeySpec(description, RIGHT_INPUT_SPEC_FIELD)) {
        return "StreamingJoin compatible savepoint only supports NoUniqueKey on both sides";
    }
    if (hasUniqueKeys(description, LEFT_UNIQUE_KEYS_FIELD) || hasUniqueKeys(description, RIGHT_UNIQUE_KEYS_FIELD)) {
        return "StreamingJoin compatible savepoint does not support unique-key join state";
    }
    if (!areStateNamesEmptyOrDefault(description)) {
        return "StreamingJoin compatible savepoint state names do not match left-records/right-records";
    }
    if (!isAllOfSupportedInputTypes(description, LEFT_INPUT_TYPES_FIELD) ||
        !isAllOfSupportedInputTypes(description, RIGHT_INPUT_TYPES_FIELD)) {
        return "StreamingJoin compatible savepoint only supports BIGINT, VARCHAR/STRING and TIMESTAMP input fields "
               "with precision <= 3";
    }
    if (!hasSupportedJoinKeys(description, LEFT_JOIN_KEY_FIELD, LEFT_INPUT_TYPES_FIELD) ||
        !hasSupportedJoinKeys(description, RIGHT_JOIN_KEY_FIELD, RIGHT_INPUT_TYPES_FIELD)) {
        return "StreamingJoin compatible savepoint requires BIGINT join keys";
    }
    if (!hasFilterNullsContract(description)) {
        return "StreamingJoin compatible savepoint requires filterNulls metadata";
    }
    return "StreamingJoin compatible savepoint adaptor is outside the current supported boundary";
}

inline StreamingJoinSavepointUtil::ParsedJoinValue StreamingJoinSavepointUtil::parseFlinkJoinValue(
    ByteView rawValue, bool outerJoinState)
{
    if (rawValue.size() < 1 + sizeof(int32_t)) {
        ERROR_RELEASE(
            "StreamingJoinSavepointUtil::parseFlinkJoinValue -> valueSize=" << rawValue.size() << ", minimumValueSize="
                                                                            << (1 + sizeof(int32_t))
                                                                            << ", outerJoinState=" << outerJoinState);
        throw std::runtime_error(
            "StreamingJoinSavepointUtil::parseFlinkJoinValue invalid Flink join value length=" +
            std::to_string(rawValue.size()));
    }

    DataInputDeserializer input(rawValue.data(), static_cast<int>(rawValue.size()), 0);
    bool isNull = input.readBoolean();
    if (isNull) {
        ERROR_RELEASE(
            "StreamingJoinSavepointUtil::parseFlinkJoinValue ->"
            << " valueSize=" << rawValue.size() << ", isNull=" << isNull << ", outerJoinState=" << outerJoinState);
        throw std::runtime_error(
            "StreamingJoinSavepointUtil::parseFlinkJoinValue null Flink join value is not supported");
    }

    ParsedJoinValue value;
    value.outerJoinState = outerJoinState;
    int remaining = input.Available();
    if (!outerJoinState && remaining == static_cast<int>(sizeof(int32_t))) {
        value.count = input.readInt();
        return value;
    }
    if (outerJoinState && remaining == static_cast<int>(sizeof(int32_t) + sizeof(int32_t))) {
        value.count = input.readInt();
        value.numAssociations = input.readInt();
        return value;
    }
    ERROR_RELEASE(
        "StreamingJoinSavepointUtil::parseFlinkJoinValue -> valueSize="
        << rawValue.size() << ", remainingBytes=" << remaining << ", outerJoinState=" << outerJoinState);
    throw std::runtime_error(
        "StreamingJoinSavepointUtil::parseFlinkJoinValue unsupported Flink join value payload length=" +
        std::to_string(remaining) + ", outerJoinState=" + std::to_string(outerJoinState));
}

inline std::shared_ptr<StateMetaInfoSnapshot> StreamingJoinSavepointUtil::createFlinkMapStateSnapshot(
    const std::string& flinkStateName,
    const StateMetaInfoSnapshot& sourceMetaInfo,
    const std::vector<std::string>& inputTypeNames,
    bool outerJoinState)
{
    TypeSerializer* namespaceSerializer = sourceMetaInfo.getNamespaceSerializer();
    if (namespaceSerializer == nullptr) {
        ERROR_RELEASE(
            "StreamingJoinSavepointUtil::createFlinkMapStateSnapshot ->"
            << " stateName=" << flinkStateName << ", namespaceSerializer=null"
            << ", inputTypeCount=" << inputTypeNames.size() << ", outerJoinState=" << outerJoinState);
        throw std::runtime_error(
            "StreamingJoinSavepointUtil::createFlinkMapStateSnapshot missing namespace serializer for state=" +
            flinkStateName);
    }
    if (namespaceSerializer->getBackendId() != BackendDataType::VOID_NAMESPACE_BK) {
        ERROR_RELEASE(
            "StreamingJoinSavepointUtil::createFlinkMapStateSnapshot ->"
            << " stateName=" << flinkStateName << ", namespaceBackendId=" << namespaceSerializer->getBackendId()
            << ", inputTypeCount=" << inputTypeNames.size());
        throw std::runtime_error(
            "StreamingJoinSavepointUtil::createFlinkMapStateSnapshot StreamingJoin requires VoidNamespace for state=" +
            flinkStateName);
    }

    BasicTypeInfo firstIntType(TYPE_NAME_INT_SERIALIZER);
    BasicTypeInfo secondIntType(TYPE_NAME_INT_SERIALIZER);
    std::vector<TypeInformation*> tupleFieldTypes{&firstIntType, &secondIntType};
    TypeSerializer* valueSerializer = outerJoinState
                                          ? static_cast<TypeSerializer*>(new Tuple2Serializer(tupleFieldTypes))
                                          : static_cast<TypeSerializer*>(new IntSerializer());
    RowType rowType(true, inputTypeNames);
    auto* stateSerializer = new MapSerializer(new RowDataSerializer(&rowType), valueSerializer);
    RegisteredKeyValueStateBackendMetaInfo convertedMetaInfo(
        StateDescriptor::Type::MAP, flinkStateName, namespaceSerializer, stateSerializer);

    return convertedMetaInfo.snapshot();
}

inline bool StreamingJoinSavepointUtil::isNoUniqueKeySpec(
    const nlohmann::json& description, const std::string& fieldName)
{
    const std::string value = getStringField(description, fieldName);
    return value == NO_UNIQUE_KEY;
}

inline bool StreamingJoinSavepointUtil::hasUniqueKeys(const nlohmann::json& description, const std::string& fieldName)
{
    if (!description.contains(fieldName) || !description[fieldName].is_array()) {
        return false;
    }
    for (const auto& key : description[fieldName]) {
        if ((key.is_array() && !key.empty()) || key.is_number_integer()) {
            return true;
        }
    }
    return false;
}

inline bool StreamingJoinSavepointUtil::areStateNamesEmptyOrDefault(const nlohmann::json& description)
{
    const std::string leftStateName = getStringField(description, LEFT_STATE_NAME_FIELD);
    const std::string rightStateName = getStringField(description, RIGHT_STATE_NAME_FIELD);
    return (leftStateName.empty() || leftStateName == LEFT_STATE_NAME) &&
           (rightStateName.empty() || rightStateName == RIGHT_STATE_NAME);
}

inline bool StreamingJoinSavepointUtil::hasSupportedJoinKeys(
    const nlohmann::json& description, const std::string& keyField, const std::string& inputTypeField)
{
    if (!description.contains(keyField) || !description[keyField].is_array() || description[keyField].empty() ||
        !description.contains(inputTypeField) || !description[inputTypeField].is_array() ||
        description[inputTypeField].empty()) {
        return false;
    }
    const auto& inputTypeNames = description[inputTypeField];
    if (!std::all_of(inputTypeNames.begin(), inputTypeNames.end(), [](const nlohmann::json& type) {
            return type.is_string() && !type.get<std::string>().empty();
        })) {
        return false;
    }
    for (const auto& key : description[keyField]) {
        if (!key.is_number_integer()) {
            return false;
        }
        const auto index = key.get<int>();
        if (index < 0 || index >= static_cast<int>(inputTypeNames.size()) || !inputTypeNames[index].is_string()) {
            return false;
        }
        const std::string typeName = inputTypeNames[index].get<std::string>();
        if (typeName.rfind("BIGINT", 0) != 0) {
            return false;
        }
    }
    return true;
}

inline bool StreamingJoinSavepointUtil::isSupportedInputType(const std::string& typeName)
{
    try {
        auto logicalTypeDeleter = [](LogicalType* logicalType) {
            if (!LogicalType::isSharedLogicalType(logicalType)) {
                delete logicalType;
            }
        };
        std::unique_ptr<LogicalType, decltype(logicalTypeDeleter)> logicalType(
            LogicalType::flinkTypeToOmniType(typeName), logicalTypeDeleter);
        const auto typeId = static_cast<omniruntime::type::DataTypeId>(logicalType->getTypeId());
        switch (typeId) {
            case omniruntime::type::DataTypeId::OMNI_LONG:
            case omniruntime::type::DataTypeId::OMNI_VARCHAR: return true;
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP:
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE: {
                auto* timestampType = dynamic_cast<TimestampWithoutTimeZoneType*>(logicalType.get());
                return timestampType != nullptr && TimestampData::isCompact(timestampType->getPrecision());
            }
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE: {
                auto* timestampType = dynamic_cast<TimestampWithLocalTimeZoneType*>(logicalType.get());
                return timestampType != nullptr && TimestampData::isCompact(timestampType->getPrecision());
            }
            default: return false;
        }
    } catch (const std::exception&) {
        // adaptor 匹配阶段不能让格式异常的类型字符串中断算子创建，统一按不支持处理并由 reason 返回诊断。
        return false;
    }
}

inline bool StreamingJoinSavepointUtil::isAllOfSupportedInputTypes(
    const nlohmann::json& description, const std::string& inputTypeField)
{
    if (!description.contains(inputTypeField) || !description[inputTypeField].is_array() ||
        description[inputTypeField].empty()) {
        return false;
    }
    const auto& inputTypeNames = description[inputTypeField];
    return std::all_of(inputTypeNames.begin(), inputTypeNames.end(), [](const nlohmann::json& type) {
        return type.is_string() && StreamingJoinSavepointUtil::isSupportedInputType(type.get<std::string>());
    });
}

inline bool StreamingJoinSavepointUtil::hasFilterNullsContract(const nlohmann::json& description)
{
    if (!description.contains(FILTER_NULLS_FIELD)) {
        return false;
    }
    const auto& filterNulls = description[FILTER_NULLS_FIELD];
    if (filterNulls.is_boolean()) {
        return true;
    }
    if (!filterNulls.is_array() || filterNulls.empty()) {
        return false;
    }
    return std::all_of(
        filterNulls.begin(), filterNulls.end(), [](const nlohmann::json& value) { return value.is_boolean(); });
}

inline bool StreamingJoinSavepointUtil::hasSupportedNoUniqueKeyJoinContract(const nlohmann::json& description)
{
    return isNoUniqueKeySpec(description, LEFT_INPUT_SPEC_FIELD) &&
           isNoUniqueKeySpec(description, RIGHT_INPUT_SPEC_FIELD) &&
           !hasUniqueKeys(description, LEFT_UNIQUE_KEYS_FIELD) &&
           !hasUniqueKeys(description, RIGHT_UNIQUE_KEYS_FIELD) && areStateNamesEmptyOrDefault(description) &&
           isAllOfSupportedInputTypes(description, LEFT_INPUT_TYPES_FIELD) &&
           isAllOfSupportedInputTypes(description, RIGHT_INPUT_TYPES_FIELD) &&
           hasSupportedJoinKeys(description, LEFT_JOIN_KEY_FIELD, LEFT_INPUT_TYPES_FIELD) &&
           hasSupportedJoinKeys(description, RIGHT_JOIN_KEY_FIELD, RIGHT_INPUT_TYPES_FIELD) &&
           hasFilterNullsContract(description);
}

// ===== WindowJoin 兼容格式适配器类型检测 =====

inline std::string StreamingJoinSavepointUtil::validateWindowJoinFields(const nlohmann::json& description)
{
    if (!description.contains(LEFT_INPUT_TYPES_FIELD) || !description.contains(RIGHT_INPUT_TYPES_FIELD)) {
        return "WindowJoin compatible savepoint requires leftInputTypes and rightInputTypes";
    }
    if (!isAllOfSupportedInputTypes(description, LEFT_INPUT_TYPES_FIELD) ||
        !isAllOfSupportedInputTypes(description, RIGHT_INPUT_TYPES_FIELD)) {
        return "WindowJoin compatible savepoint only supports BIGINT, VARCHAR/STRING and TIMESTAMP input fields";
    }
    if (!description.contains(LEFT_JOIN_KEY_FIELD) || !description.contains(RIGHT_JOIN_KEY_FIELD)) {
        return "WindowJoin compatible savepoint requires leftJoinKey and rightJoinKey fields";
    }
    if (!description.contains(LEFT_WINDOW_END_INDEX_FIELD) || !description.contains(RIGHT_WINDOW_END_INDEX_FIELD)) {
        return "WindowJoin compatible savepoint requires leftWindowEndIndex and rightWindowEndIndex";
    }
    return "";
}

inline FlinkSavepointAdaptorType StreamingJoinSavepointUtil::getWindowJoinAdaptorType(const nlohmann::json& description)
{
    // 验证 WindowJoin 特有字段完整性
    if (!validateWindowJoinFields(description).empty()) {
        return FlinkSavepointAdaptorType::None;
    }

    return FlinkSavepointAdaptorType::OmniIsCompatible;
}

inline std::string StreamingJoinSavepointUtil::buildWindowJoinUnsupportedReason(const nlohmann::json& description)
{
    const std::string reason = validateWindowJoinFields(description);
    if (!reason.empty()) {
        return reason;
    }
    return "WindowJoin compatible savepoint adaptor is outside the current supported boundary";
}

} // namespace omnistream
