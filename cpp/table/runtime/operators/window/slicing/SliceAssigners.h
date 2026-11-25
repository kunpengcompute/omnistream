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

#ifndef FLINK_TNEL_SLICEASSIGNERS_H
#define FLINK_TNEL_SLICEASSIGNERS_H

#include "chrono"
#include "memory"
#include "vector"
#include "stdexcept"
#include "functional"
#include "string"
#include "algorithm"
#include "optional"
#include "numeric"
#include "table/data/vectorbatch/VectorBatch.h"
#include "iostream"
#include "iterator"
#include "cstddef"
#include "nlohmann/json.hpp"
#include "OmniOperatorJIT/core/src/vector/vector.h"

using namespace std::chrono;

// ------—------—------—------—------—------—------—------—------—------—
// 基础类型定义
// ------—------—------—------—------—------—------—------—------—------—

using TimePoint = system_clock::time_point;

// 模拟时钟服务
class ClockService {
public:
    ClockService()
    {
    }
    TimePoint currentProcessingTime() const
    {
        return system_clock::now();
    }
};

class IteratorBase {
public:
    virtual bool hasNext() const = 0;
    virtual int64_t next() = 0;
    virtual std::unique_ptr<IteratorBase> iterator() = 0;
    virtual ~IteratorBase() = default;
    virtual std::vector<int64_t> getList() = 0;
};
namespace omnistream {
class ZoneId {
public:
    ZoneId() : id_("UTC")
    {
    }
    explicit ZoneId(const std::string& id) : id_(id.empty() ? "UTC" : id)
    {
    }
    ~ZoneId() = default;
    const std::string& getId() const
    {
        return id_;
    }

private:
    std::string id_;
};
} // namespace omnistream

class SliceAssigner {
public:
    virtual ~SliceAssigner() = default;
    virtual int64_t assignSliceEnd(omnistream::VectorBatch* element, int rowId, ClockService* clock) = 0;
    virtual int64_t getLastWindowEnd(int64_t sliceEnd) = 0;
    virtual int64_t getWindowStart(int64_t windowEnd) = 0;
    virtual IteratorBase* expiredSlices(int64_t windowEnd) = 0;
    virtual int64_t getSliceEndInterval() = 0;
    virtual bool isEventTime() = 0;
    virtual void SetWindowEnd(bool hasIndex) = 0;
    virtual void SetSliceEnd(bool hasIndex) = 0;
    virtual int64_t GetCurrentTimeStamp(
        omnistream::VectorBatch* element, int rowId, ClockService* clock, int rowTimeIdx) = 0;
    bool hasWindowEndIndex = false;
    bool hasSliceEndIndex = false;
};

class AbstractSlicedSliceAssigner : public virtual SliceAssigner {
public:
    AbstractSlicedSliceAssigner(SliceAssigner* innerAssigner, int sliceEndIndex);
    ~AbstractSlicedSliceAssigner() override
    {
        delete innerAssigner;
    };
    int64_t assignSliceEnd(omnistream::VectorBatch* element, int rowId, ClockService* clock) override;
    int64_t getWindowStart(int64_t windowEnd) override;
    IteratorBase* expiredSlices(int64_t windowEnd) override;
    int64_t getSliceEndInterval() override;
    bool isEventTime() override;

protected:
    SliceAssigner* innerAssigner;

private:
    int sliceEndIndex;
};

class AbstractSliceAssigner : public virtual SliceAssigner {
public:
    ~AbstractSliceAssigner() override
    {
        delete shiftTimeZone;
    }
    int64_t assignSliceEnd(omnistream::VectorBatch* element, int rowId, ClockService* clock) override;
    bool isEventTime() override;
    std::string getShiftTimeZoneId() const;
    virtual int64_t assignSliceEnd(int64_t timestamp) = 0;

protected:
    int rowtimeIndex;
    bool isEventTimeBool;
    omnistream::ZoneId* shiftTimeZone;

    // 构造函数受保护，防止直接实例化
    AbstractSliceAssigner(int rowtimeIndex, omnistream::ZoneId* shiftTimeZone)
        : rowtimeIndex(rowtimeIndex),
          shiftTimeZone(shiftTimeZone)
    {
        this->isEventTimeBool = rowtimeIndex >= 0;
    }
};

class MergeCallback {
public:
    virtual void merge(long mergeResult, IteratorBase* toBeMerged) = 0;
};

class ReusableListIterable : public IteratorBase {
public:
    void clear();
    void reset(int64_t slice);
    void reset(int64_t slice1, int64_t slice2);
    bool hasNext() const override;
    int64_t next() override;
    std::unique_ptr<IteratorBase> iterator();
    std::vector<int64_t> getList();

private:
    std::vector<int64_t> values;
    size_t index = 0;
};

// 跳跃切片迭代器
class HoppingSlicesIterable : public IteratorBase {
public:
    HoppingSlicesIterable(int64_t last_slice_end, int64_t slice_size, int num_slices_per_window);
    bool hasNext() const override;
    int64_t next() override;
    std::unique_ptr<IteratorBase> iterator() override;
    std::vector<int64_t> getList() override
    {
        return {};
    };

private:
    const int64_t slice_size;
    int64_t current_value;
    int remaining;
};

class SliceSharedAssigner : public virtual SliceAssigner {
public:
    ~SliceSharedAssigner() override = default;

    // 纯虚函数声明
    virtual void mergeSlices(int64_t* sliceEnd, MergeCallback* callback) = 0;
    virtual int64_t NextTriggerWindow(int64_t windowEnd, bool hasCountFunc) = 0;
    void SetWindowEnd(bool hasIndex) override
    {
    }
    void SetSliceEnd(bool hasIndex) override
    {
    }
};

class SlicedSharedSliceAssigner : public AbstractSlicedSliceAssigner, public SliceSharedAssigner {
public:
    SlicedSharedSliceAssigner(int sliceEndIndex, SliceSharedAssigner* innerAssigner);
    void mergeSlices(int64_t* sliceEnd, MergeCallback* callback) override;
    int64_t NextTriggerWindow(int64_t windowEnd, bool hasCountFunc) override;
    int64_t getLastWindowEnd(int64_t sliceEnd) override;

private:
    SliceSharedAssigner* innerSharedAssigner;
};

// ------—------—------—------—------—------—------—------—------—------—
// 工具函数
// ------—------—------—------—------—------—------—------—------—------—

// to use TimeWindow class
class TimeWindow1 {
public:
    static int64_t getWindowStartWithOffset(int64_t timestamp, int64_t offset, int64_t size);
    static int64_t GetCurrentTimeStamp(
        omnistream::VectorBatch* element, int rowId, ClockService* clock, int rowtimeIndex);
};

class SliceUnsharedAssigner : public virtual SliceAssigner {
    void SetWindowEnd(bool hasIndex) override
    {
        hasWindowEndIndex = hasIndex;
    }

    void SetSliceEnd(bool hasIndex) override
    {
        hasSliceEndIndex = hasIndex;
    }
};

class WindowedSliceAssigner : public SliceUnsharedAssigner {
public:
    WindowedSliceAssigner(int windowEndIndex, SliceAssigner* innerAssigner)
        : windowEndIndex(windowEndIndex),
          innerAssigner(innerAssigner)
    {
    }

    ~WindowedSliceAssigner() override
    {
        delete innerAssigner;
        delete reuseExpiredList;
    };

    int64_t assignSliceEnd(omnistream::VectorBatch* element, int rowId, ClockService* clock)
    {
        return reinterpret_cast<omniruntime::vec::Vector<std::int64_t>*>(element->GetVectors()[windowEndIndex])
            ->GetValue(rowId);
    }

    int64_t getLastWindowEnd(int64_t sliceEnd) override
    {
        return sliceEnd;
    }

    int64_t getWindowStart(int64_t windowEnd) override
    {
        return innerAssigner->getWindowStart(windowEnd);
    }

    IteratorBase* expiredSlices(int64_t windowEnd) override
    {
        reuseExpiredList->reset(windowEnd);
        return reuseExpiredList;
    }

    int64_t getSliceEndInterval() override
    {
        return innerAssigner->getSliceEndInterval();
    }

    bool isEventTime() override
    {
        return true;
    }

    SliceAssigner* GetInnerAssigner()
    {
        return innerAssigner;
    }

    int64_t GetCurrentTimeStamp(
        omnistream::VectorBatch* element, int rowId, ClockService* clock, int rowTimeIdx) override
    {
        return 0;
    };

private:
    int windowEndIndex;
    SliceAssigner* innerAssigner;
    ReusableListIterable* reuseExpiredList = new ReusableListIterable();
};

class SlicedUnsharedSliceAssigner : public AbstractSlicedSliceAssigner, public SliceUnsharedAssigner {
public:
    SlicedUnsharedSliceAssigner(int sliceEndIndex, SliceAssigner* innerAssigner);
    ~SlicedUnsharedSliceAssigner() override = default;
    int64_t getLastWindowEnd(int64_t sliceEnd) override;
};

// ------—------—------—------—------—------—------—------—------—------—
// TumblingSliceAssigner（滚动窗口分配器）
// ------—------—------—------—------—------—------—------—------—------—

class TumblingSliceAssigner : public AbstractSliceAssigner, public SliceUnsharedAssigner {
public:
    TumblingSliceAssigner(int rowtimeIndex, omnistream::ZoneId* shiftTimeZone, int64_t size, int64_t offset);
    ~TumblingSliceAssigner() override;
    TumblingSliceAssigner* withOffset(int64_t offset);
    int64_t assignSliceEnd(omnistream::VectorBatch* element, int rowId, ClockService* clock) override;
    int64_t assignSliceEnd(int64_t timestamp) override;
    int64_t getLastWindowEnd(int64_t sliceEnd) override;
    int64_t getWindowStart(int64_t windowEnd) override;
    IteratorBase* expiredSlices(int64_t windowEnd) override;
    int64_t getSliceEndInterval() override;
    bool isEventTime() override;
    int64_t GetCurrentTimeStamp(
        omnistream::VectorBatch* element, int rowId, ClockService* clock, int rowTimeIdx) override;

private:
    int64_t size;
    int64_t offset;
    ReusableListIterable* reuseExpiredList;
};

class HoppingSliceAssigner : public AbstractSliceAssigner, public SliceSharedAssigner {
public:
    HoppingSliceAssigner(
        int rowtimeIndex, omnistream::ZoneId* shiftTimeZone, int64_t size, int64_t slide, int64_t offset);
    ~HoppingSliceAssigner() override;
    HoppingSliceAssigner* withOffset(int64_t offset);
    int64_t assignSliceEnd(omnistream::VectorBatch* element, int rowId, ClockService* clock) override;
    int64_t assignSliceEnd(int64_t timestamp) override;
    int64_t getLastWindowEnd(int64_t sliceEnd) override;
    int64_t getWindowStart(int64_t windowEnd) override;
    IteratorBase* expiredSlices(int64_t windowEnd) override;
    int64_t getSliceEndInterval() override;
    bool isEventTime() override;
    void mergeSlices(int64_t* sliceEnd, MergeCallback* callback) override;
    int64_t NextTriggerWindow(int64_t windowEnd, bool windowIsEmpty) override;
    int64_t getNumSlicesPerWindow();
    int64_t GetCurrentTimeStamp(
        omnistream::VectorBatch* element, int rowId, ClockService* clock, int rowTimeIdx) override;

private:
    int64_t size;
    int64_t slide;
    int64_t offset;
    int64_t sliceSize;
    std::int64_t numSlicesPerWindow;
    ReusableListIterable* reuseExpiredList;
};

// ------—------—------—------—------—------—------—------—------—------—
// CumulativeSliceAssigner（累积窗口分配器）
// ------—------—------—------—------—------—------—------—------—------—

class CumulativeSliceAssigner : public AbstractSliceAssigner, public SliceSharedAssigner {
public:
    CumulativeSliceAssigner(
        int rowtimeIndex, omnistream::ZoneId* shiftTimeZone, int64_t maxSize, int64_t step, int64_t offset);
    ~CumulativeSliceAssigner() override;
    CumulativeSliceAssigner* withOffset(int64_t offset);
    int64_t assignSliceEnd(omnistream::VectorBatch* element, int rowId, ClockService* clock) override;
    int64_t assignSliceEnd(int64_t timestamp) override;
    int64_t getLastWindowEnd(int64_t sliceEnd) override;
    int64_t getWindowStart(int64_t windowEnd) override;
    IteratorBase* expiredSlices(int64_t windowEnd) override;
    int64_t getSliceEndInterval() override;
    void mergeSlices(int64_t* sliceEnd, MergeCallback* callback) override;
    int64_t NextTriggerWindow(int64_t windowEnd, bool hasCountFunc) override;
    bool isEventTime() override;
    int64_t GetCurrentTimeStamp(
        omnistream::VectorBatch* element, int rowId, ClockService* clock, int rowTimeIdx) override;

private:
    int64_t maxSize;
    int64_t step;
    int64_t offset;
    ReusableListIterable* reuseToBeMergedList;
    ReusableListIterable* reuseExpiredList;
};

// ------—------—------—------—------—------—------—------—------—------—
// SliceAssigners 工厂类
// ------—------—------—------—------—------—------—------—------—------—

class SliceAssigners {
public:
    // 禁止实例化（工具类）
    SliceAssigners() = delete;

    static TumblingSliceAssigner* tumbling(int rowtimeIndex, omnistream::ZoneId* shiftTimeZone, int64_t size)
    {
        return new TumblingSliceAssigner(rowtimeIndex, shiftTimeZone, size, 0);
    }

    static HoppingSliceAssigner* hopping(
        int rowtimeIndex, omnistream::ZoneId* shiftTimeZone, std::int64_t size, std::int64_t slide)
    {
        return new HoppingSliceAssigner(rowtimeIndex, shiftTimeZone, size, slide, 0);
    }

    static CumulativeSliceAssigner* cumulative(
        int rowtimeIndex, omnistream::ZoneId* shiftTimeZone, int64_t maxSize, int64_t step)
    {
        return new CumulativeSliceAssigner(rowtimeIndex, shiftTimeZone, maxSize, step, 0);
    }
};

inline std::string ResolveShiftTimeZoneId(SliceAssigner* assigner)
{
    if (assigner == nullptr) {
        return "UTC";
    }
    if (auto* abstractAssigner = dynamic_cast<AbstractSliceAssigner*>(assigner)) {
        return abstractAssigner->getShiftTimeZoneId();
    }
    if (auto* windowedAssigner = dynamic_cast<WindowedSliceAssigner*>(assigner)) {
        return ResolveShiftTimeZoneId(windowedAssigner->GetInnerAssigner());
    }
    return "UTC";
}

class AssignerAtt {
public:
    static SliceAssigner* createSliceAssigner(const nlohmann::json& parsedJson);
    static HoppingSliceAssigner* CreateHopSliceAssigner(
        const nlohmann::json& parsedJson, int rowtimeIndexVal, omnistream::ZoneId* zonePtr);
    static TumblingSliceAssigner* CreateTumbleSliceAssigner(
        const nlohmann::json& parsedJson, int rowtimeIndexVal, omnistream::ZoneId* zonePtr);
    static CumulativeSliceAssigner* CreateCumlativeSliceAssigner(
        const nlohmann::json& parsedJson, int rowtimeIndexVal, omnistream::ZoneId* zonePtr);
};

#endif
