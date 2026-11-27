#include <gtest/gtest.h>
#include "streaming/api/operators/AbstractUdfStreamOperator.h"

namespace {
class CloseTrackedFunction : public AbstractRichFunction {
public:
    explicit CloseTrackedFunction(int& calls) : calls_(calls)
    {
    }
    void close() override
    {
        ++calls_;
    }

private:
    int& calls_;
};

template <typename F>
class CloseTestOperator : public AbstractUdfStreamOperator<F, int> {
public:
    explicit CloseTestOperator(F* function) : AbstractUdfStreamOperator<F, int>(function)
    {
    }
};
} // namespace

TEST(AbstractUdfStreamOperatorCloseTest, DispatchesRichFunctionCloseOnce)
{
    int calls = 0;
    {
        CloseTestOperator<CloseTrackedFunction> op(new CloseTrackedFunction(calls));
        op.setup();
        op.open();
        op.close();
        EXPECT_EQ(calls, 1);
    }
    EXPECT_EQ(calls, 1);
}

TEST(AbstractUdfStreamOperatorCloseTest, NonRichAndNullFunctionsAreSkipped)
{
    CloseTestOperator<omnistream::Function> plain(new omnistream::Function());
    plain.setup();
    EXPECT_NO_THROW(plain.close());
    CloseTestOperator<omnistream::Function> empty(nullptr);
    empty.setup();
    EXPECT_NO_THROW(empty.close());
}

TEST(AbstractUdfStreamOperatorCloseTest, CloseAfterPartialOpenIsForwarded)
{
    class FailedOpen : public CloseTrackedFunction {
    public:
        using CloseTrackedFunction::CloseTrackedFunction;
        void open(const Configuration&) override
        {
            throw std::runtime_error("open failed");
        }
    };
    int calls = 0;
    CloseTestOperator<FailedOpen> op(new FailedOpen(calls));
    op.setup();
    EXPECT_THROW(op.open(), std::runtime_error);
    op.close();
    EXPECT_EQ(calls, 1);
}

TEST(AbstractUdfStreamOperatorCloseTest, InheritedEmptyRichFunctionCloseIsSafe)
{
    class EmptyRichFunction : public AbstractRichFunction {};
    CloseTestOperator<EmptyRichFunction> op(new EmptyRichFunction());
    op.setup();
    op.open();
    EXPECT_NO_THROW(op.close());
}
