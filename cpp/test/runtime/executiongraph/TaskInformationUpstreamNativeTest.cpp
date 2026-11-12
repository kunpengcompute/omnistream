#include "gtest/gtest.h"
#include "runtime/executiongraph/TaskInformationPOD.h"

using namespace omnistream;

TEST(TaskInformationUpstreamNativeTest, ReadsJavaMetadataAndPreservesPhysicalGateOrder)
{
    nlohmann::json metadata = {{"upstreamNative", "[true, false, true]"}};
    auto task = metadata.get<TaskInformationPOD>();
    EXPECT_EQ(task.getUpstreamNative(), (std::vector<bool>{true, false, true}));
    EXPECT_NO_THROW(task.validateInputGateCount(3));
    EXPECT_THROW(task.validateInputGateCount(2), std::invalid_argument);
}

TEST(TaskInformationUpstreamNativeTest, EmptyMetadataIsOnlyValidForSourceTasks)
{
    auto task = nlohmann::json::object().get<TaskInformationPOD>();
    EXPECT_NO_THROW(task.validateInputGateCount(0));
    EXPECT_THROW(task.validateInputGateCount(1), std::invalid_argument);
}

TEST(TaskInformationUpstreamNativeTest, RejectsNonBooleanAndNonArrayMetadata)
{
    for (const auto* value : {"true", "[true, 1]", "[null]", "[\"false\"]"}) {
        nlohmann::json metadata = {{"upstreamNative", value}};
        EXPECT_THROW(metadata.get<TaskInformationPOD>(), std::invalid_argument);
    }
    nlohmann::json malformed = {{"upstreamNative", "[true,"}};
    EXPECT_THROW(malformed.get<TaskInformationPOD>(), nlohmann::json::parse_error);
}
