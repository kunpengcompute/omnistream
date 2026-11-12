#include "gtest/gtest.h"
#include "runtime/partition/consumer/InputFormat.h"

using namespace omnistream;

TEST(InputFormatTest, RejectsConsumptionBeforeDeployment)
{
    InputFormatState state;
    EXPECT_THROW(state.get(), std::logic_error);
}

TEST(InputFormatTest, KeepsDifferentInputsIndependentAndRejectsReconfiguration)
{
    InputFormatState javaInput;
    InputFormatState omniInput;
    javaInput.initialize(InputFormat::FLINK_ROW);
    omniInput.initialize(InputFormat::OMNI_BATCH);
    EXPECT_EQ(javaInput.get(), InputFormat::FLINK_ROW);
    EXPECT_EQ(omniInput.get(), InputFormat::OMNI_BATCH);
    EXPECT_THROW(javaInput.initialize(InputFormat::OMNI_BATCH), std::logic_error);
    EXPECT_THROW(omniInput.initialize(InputFormat::OMNI_BATCH), std::logic_error);
    EXPECT_EQ(javaInput.get(), InputFormat::FLINK_ROW);
}
