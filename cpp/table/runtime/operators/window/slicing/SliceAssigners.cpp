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

#include "SliceAssigners.h"
#include "table/utils/TimeWindowUtil.h"

int64_t TimeWindow1::getWindowStartWithOffset(int64_t timestamp, int64_t offset, int64_t size)
{
    long remainder = (timestamp - offset) % size;
    if (remainder < 0) {
        remainder += size;
    }
    return timestamp - remainder;
};

int64_t TimeWindow1::GetCurrentTimeStamp(
    omnistream::VectorBatch* element, int rowId, ClockService* clock, int rowtimeIndex)
{
    int64_t timestamp;
    if (rowtimeIndex >= 0) {
        timestamp = reinterpret_cast<omniruntime::vec::Vector<std::int64_t>*>(element->GetVectors()[rowtimeIndex])
                        ->GetValue(rowId);
    } else {
        timestamp = duration_cast<milliseconds>(clock->currentProcessingTime().time_since_epoch()).count();
    }
    return timestamp;
}

AbstractSlicedSliceAssigner::AbstractSlicedSliceAssigner(SliceAssigner* innerAssigner, int sliceEndIndex)
    : innerAssigner(innerAssigner),
      sliceEndIndex(sliceEndIndex) {};

int64_t AbstractSlicedSliceAssigner::assignSliceEnd(omnistream::VectorBatch* element, int rowId, ClockService* clock)
{
    return reinterpret_cast<omniruntime::vec::Vector<std::int64_t>*>(element->GetVectors()[sliceEndIndex])
        ->GetValue(rowId);
};

int64_t AbstractSlicedSliceAssigner::getWindowStart(int64_t windowEnd)
{
    return innerAssigner->getWindowStart(windowEnd);
};

IteratorBase* AbstractSlicedSliceAssigner::expiredSlices(int64_t windowEnd)
{
    return innerAssigner->expiredSlices(windowEnd);
};

int64_t AbstractSlicedSliceAssigner::getSliceEndInterval()
{
    return innerAssigner->getSliceEndInterval();
};

bool AbstractSlicedSliceAssigner::isEventTime()
{
    return true;
};

bool AbstractSliceAssigner::isEventTime()
{
    return isEventTimeBool;
};

std::string AbstractSliceAssigner::getShiftTimeZoneId() const
{
    if (shiftTimeZone == nullptr) {
        return "UTC";
    }
    return shiftTimeZone->getId();
}

int64_t AbstractSliceAssigner::assignSliceEnd(omnistream::VectorBatch* element, int rowId, ClockService* clock)
{
    long timestamp;
    if (rowtimeIndex >= 0) {
        timestamp = reinterpret_cast<omniruntime::vec::Vector<std::int64_t>*>(element->GetVectors()[rowtimeIndex])
                        ->GetValue(rowId);
    } else {
        timestamp = duration_cast<milliseconds>(clock->currentProcessingTime().time_since_epoch()).count();
    }
    timestamp = TimeWindowUtil::toUtcTimestampMills(timestamp, getShiftTimeZoneId());
    return assignSliceEnd(timestamp);
};

bool CumulativeSliceAssigner::isEventTime()
{
    return isEventTimeBool;
};

SlicedSharedSliceAssigner::SlicedSharedSliceAssigner(int sliceEndIndex, SliceSharedAssigner* innerAssigner)
    : AbstractSlicedSliceAssigner(innerAssigner, sliceEndIndex),
      innerSharedAssigner(innerAssigner) {};

void SlicedSharedSliceAssigner::mergeSlices(int64_t* sliceEnd, MergeCallback* callback)
{
    innerSharedAssigner->mergeSlices(sliceEnd, callback);
};

int64_t SlicedSharedSliceAssigner::NextTriggerWindow(int64_t windowEnd, bool hasCountFunc)
{
    return innerSharedAssigner->NextTriggerWindow(windowEnd, hasCountFunc);
};

int64_t SlicedSharedSliceAssigner::getLastWindowEnd(int64_t sliceEnd)
{
    return innerAssigner->getLastWindowEnd(sliceEnd);
};

TumblingSliceAssigner::TumblingSliceAssigner(
    int rowtimeIndex, omnistream::ZoneId* shiftTimeZone, int64_t size, int64_t offset)
    : AbstractSliceAssigner(rowtimeIndex, shiftTimeZone),
      size(size),
      offset(offset)
{
    if (size <= 0) {
        throw std::invalid_argument("Size must be positive");
    }
    if (abs(offset) >= size) {
        throw std::invalid_argument("Offset must be smaller than size");
    }
    this->size = size;
    this->offset = offset;
    this->reuseExpiredList = new ReusableListIterable();
}

TumblingSliceAssigner::~TumblingSliceAssigner()
{
    delete reuseExpiredList;
}

int64_t TumblingSliceAssigner::assignSliceEnd(int64_t timestamp)
{
    int64_t start = TimeWindow1::getWindowStartWithOffset(timestamp, offset, size);
    return start + size;
};

int64_t TumblingSliceAssigner::assignSliceEnd(omnistream::VectorBatch* element, int rowId, ClockService* clock)
{
    return AbstractSliceAssigner::assignSliceEnd(element, rowId, clock);
};

TumblingSliceAssigner* TumblingSliceAssigner::withOffset(int64_t offset_)
{
    return new TumblingSliceAssigner(rowtimeIndex, shiftTimeZone, size, offset_);
}

int64_t TumblingSliceAssigner::getLastWindowEnd(int64_t sliceEnd)
{
    return sliceEnd;
}

int64_t TumblingSliceAssigner::getWindowStart(int64_t windowEnd)
{
    return windowEnd - size;
}

IteratorBase* TumblingSliceAssigner::expiredSlices(int64_t windowEnd)
{
    reuseExpiredList->reset(windowEnd);
    return reuseExpiredList;
}

int64_t TumblingSliceAssigner::getSliceEndInterval()
{
    return size;
}

bool TumblingSliceAssigner::isEventTime()
{
    return isEventTimeBool;
};

int64_t TumblingSliceAssigner::GetCurrentTimeStamp(
    omnistream::VectorBatch* element, int rowId, ClockService* clock, int rowTimeIdx)
{
    return TimeWindow1::GetCurrentTimeStamp(element, rowId, clock, rowTimeIdx);
}

HoppingSliceAssigner::HoppingSliceAssigner(
    int rowtimeIndex, omnistream::ZoneId* shiftTimeZone, int64_t size, int64_t slide, int64_t offset)
    : AbstractSliceAssigner(rowtimeIndex, shiftTimeZone),
      size(size),
      slide(slide),
      offset(offset),
      sliceSize(std::gcd(size, slide))
{
    if (size <= 0 || slide <= 0) {
        throw std::invalid_argument("Size and slide must be positive");
    }
    if (size % slide != 0) {
        throw std::invalid_argument("Size must be multiple of slide");
    }
    numSlicesPerWindow = size / sliceSize;
    reuseExpiredList = new ReusableListIterable();
}

HoppingSliceAssigner::~HoppingSliceAssigner()
{
    delete reuseExpiredList;
}

HoppingSliceAssigner* HoppingSliceAssigner::withOffset(int64_t offset_)
{
    return new HoppingSliceAssigner(rowtimeIndex, shiftTimeZone, size, slide, offset_);
}

int64_t HoppingSliceAssigner::GetCurrentTimeStamp(
    omnistream::VectorBatch* element, int rowId, ClockService* clock, int rowTimeIdx)
{
    return TimeWindow1::GetCurrentTimeStamp(element, rowId, clock, rowTimeIdx);
}

int64_t HoppingSliceAssigner::assignSliceEnd(omnistream::VectorBatch* element, int rowId, ClockService* clock)
{
    return AbstractSliceAssigner::assignSliceEnd(element, rowId, clock);
}

int64_t HoppingSliceAssigner::assignSliceEnd(int64_t timestamp)
{
    int64_t remainder = (timestamp - offset) % sliceSize;
    if (remainder < 0) {
        return timestamp - remainder;
    } else {
        return timestamp - remainder + sliceSize;
    }
};

int64_t HoppingSliceAssigner::getLastWindowEnd(int64_t sliceEnd)
{
    return sliceEnd - sliceSize + size;
}

int64_t HoppingSliceAssigner::getWindowStart(int64_t windowEnd)
{
    return windowEnd - size;
}

IteratorBase* HoppingSliceAssigner::expiredSlices(int64_t windowEnd)
{
    int64_t windowStart = getWindowStart(windowEnd);
    int64_t firstSliceEnd = windowStart + sliceSize;
    reuseExpiredList->reset(firstSliceEnd);
    return reuseExpiredList;
}

int64_t HoppingSliceAssigner::getSliceEndInterval()
{
    return sliceSize;
}

bool HoppingSliceAssigner::isEventTime()
{
    return isEventTimeBool;
};

void ReusableListIterable::clear()
{
    values.clear();
    index = 0;
}

void ReusableListIterable::reset(int64_t slice)
{
    values = {slice};
    index = 0;
}

void ReusableListIterable::reset(int64_t slice1, int64_t slice2)
{
    values = {slice1, slice2};
    index = 0;
}

bool ReusableListIterable::hasNext() const
{
    return index < values.size();
}

int64_t ReusableListIterable::next()
{
    if (!hasNext()) {
        throw std::out_of_range("No more elements");
    }
    return values[index++];
}

std::unique_ptr<IteratorBase> ReusableListIterable::iterator()
{
    auto copy = std::make_unique<ReusableListIterable>(*this);
    copy->index = 0; // 重置索引
    return copy;
}

std::vector<int64_t> ReusableListIterable::getList()
{
    return values;
}

HoppingSlicesIterable::HoppingSlicesIterable(int64_t last_slice_end, int64_t slice_size, int num_slices_per_window)
    : slice_size(slice_size),
      current_value(last_slice_end),
      remaining(num_slices_per_window)
{
}

bool HoppingSlicesIterable::hasNext() const
{
    return remaining > 0;
}

int64_t HoppingSlicesIterable::next()
{
    if (!hasNext()) {
        throw std::out_of_range("No more elements");
    }
    const int64_t val = current_value;
    current_value -= slice_size;
    --remaining;
    return val;
}

std::unique_ptr<IteratorBase> HoppingSlicesIterable::iterator()
{
    return std::make_unique<HoppingSlicesIterable>(*this);
}

void HoppingSliceAssigner::mergeSlices(int64_t* sliceEnd, MergeCallback* callback)
{
    auto toBeMerged = new HoppingSlicesIterable(*sliceEnd, sliceSize, numSlicesPerWindow);
    callback->merge(0, toBeMerged);
}

int64_t HoppingSliceAssigner::NextTriggerWindow(int64_t windowEnd, bool windowIsEmpty)
{
    if (windowIsEmpty) {
        return 0;
    } else {
        return windowEnd + sliceSize;
    }
}

int64_t HoppingSliceAssigner::getNumSlicesPerWindow()
{
    return numSlicesPerWindow;
}

CumulativeSliceAssigner::CumulativeSliceAssigner(
    int rowtimeIndex, omnistream::ZoneId* shiftTimeZone, int64_t maxSize, int64_t step, int64_t offset)
    : AbstractSliceAssigner(rowtimeIndex, shiftTimeZone),
      maxSize(maxSize),
      step(step),
      offset(offset)
{
    if (maxSize <= 0 || step <= 0) {
        throw std::invalid_argument("MaxSize and step must be positive");
    }
    if (maxSize % step != 0) {
        throw std::invalid_argument("MaxSize must be multiple of step");
    }
    reuseToBeMergedList = new ReusableListIterable();
    reuseExpiredList = new ReusableListIterable();
}

CumulativeSliceAssigner::~CumulativeSliceAssigner()
{
    delete reuseToBeMergedList;
    delete reuseExpiredList;
}

CumulativeSliceAssigner* CumulativeSliceAssigner::withOffset(int64_t offset_)
{
    return new CumulativeSliceAssigner(rowtimeIndex, shiftTimeZone, maxSize, step, offset_);
}

int64_t CumulativeSliceAssigner::assignSliceEnd(omnistream::VectorBatch* element, int rowId, ClockService* clock)
{
    return AbstractSliceAssigner::assignSliceEnd(element, rowId, clock);
}

int64_t CumulativeSliceAssigner::assignSliceEnd(int64_t timestamp)
{
    int64_t start = TimeWindow1::getWindowStartWithOffset(timestamp, offset, step);
    return start + step;
}

int64_t CumulativeSliceAssigner::getLastWindowEnd(int64_t sliceEnd)
{
    int64_t windowStart = getWindowStart(sliceEnd);
    return windowStart + maxSize;
}

int64_t CumulativeSliceAssigner::getWindowStart(int64_t windowEnd)
{
    return TimeWindow1::getWindowStartWithOffset(windowEnd - 1, offset, maxSize);
}

IteratorBase* CumulativeSliceAssigner::expiredSlices(int64_t windowEnd)
{
    int64_t windowStart = getWindowStart(windowEnd);
    int64_t firstSliceEnd = windowStart + step;
    int64_t lastSliceEnd = windowStart + maxSize;
    if (windowEnd == firstSliceEnd) {
        reuseExpiredList->clear();
    } else if (windowEnd == lastSliceEnd) {
        reuseExpiredList->reset(windowEnd, firstSliceEnd);
    } else {
        // clean up current slice
        reuseExpiredList->reset(windowEnd);
    }
    return reuseExpiredList;
}

int64_t CumulativeSliceAssigner::getSliceEndInterval()
{
    return step;
}

void CumulativeSliceAssigner::mergeSlices(int64_t* sliceEnd, MergeCallback* callback)
{
    int64_t windowStart = getWindowStart(*sliceEnd);
    int64_t firstSliceEnd = windowStart + step;
    if (*sliceEnd == firstSliceEnd) {
        reuseToBeMergedList->clear();
    } else {
        reuseToBeMergedList->reset(*sliceEnd);
    }
    callback->merge(firstSliceEnd, reuseToBeMergedList);
}

int64_t CumulativeSliceAssigner::NextTriggerWindow(int64_t windowEnd, bool hasCountFunc)
{
    int64_t nextWindowEnd = windowEnd + step;
    int64_t maxWindowEnd = getWindowStart(windowEnd) + maxSize;
    if (nextWindowEnd > maxWindowEnd) {
        return 0;
    } else {
        return nextWindowEnd;
    }
}

int64_t CumulativeSliceAssigner::GetCurrentTimeStamp(
    omnistream::VectorBatch* element, int rowId, ClockService* clock, int rowTimeIdx)
{
    return TimeWindow1::GetCurrentTimeStamp(element, rowId, clock, rowTimeIdx);
}

SlicedUnsharedSliceAssigner::SlicedUnsharedSliceAssigner(int sliceEndIndex, SliceAssigner* innerAssigner)
    : AbstractSlicedSliceAssigner(innerAssigner, sliceEndIndex) {};

int64_t SlicedUnsharedSliceAssigner::getLastWindowEnd(int64_t sliceEnd)
{
    return sliceEnd;
}

SliceAssigner* AssignerAtt::createSliceAssigner(const nlohmann::json& parsedJson)
{
    nlohmann::json windowing = parsedJson["window"];
    auto windowMsgStr = windowing.get<std::string>();
    auto rowtimeIndexVal = -1;
    if (parsedJson.contains("timeAttributeIndex")) {
        nlohmann::json rowtimeIndex = parsedJson["timeAttributeIndex"];
        rowtimeIndexVal = rowtimeIndex.get<long>();
    }
    std::string shiftTz = "UTC";
    if (parsedJson.contains("shiftTimeZone")) {
        shiftTz = parsedJson["shiftTimeZone"].get<std::string>();
    }
    auto zonePtr = new omnistream::ZoneId(shiftTz);
    size_t windowTypeIndex = windowMsgStr.find('(');
    auto windowTypeStr = windowMsgStr.substr(0, windowTypeIndex);

    SliceAssigner* sliceAssigner = nullptr;
    if (windowTypeStr == "HOP") {
        sliceAssigner = CreateHopSliceAssigner(parsedJson, rowtimeIndexVal, zonePtr);
    } else if (windowTypeStr == "TUMBLE") {
        sliceAssigner = CreateTumbleSliceAssigner(parsedJson, rowtimeIndexVal, zonePtr);
    } else if (windowTypeStr == "CUMULATE") {
        sliceAssigner = CreateCumlativeSliceAssigner(parsedJson, rowtimeIndexVal, zonePtr);
    } else {
        // not support window type
        THROW_LOGIC_EXCEPTION("wrong slice assigner type passed");
    }

    if (parsedJson.contains("windowEndIndex")) {
        LOG("windowEndIndex in assigner");
        sliceAssigner = new WindowedSliceAssigner(parsedJson["windowEndIndex"].get<int>(), sliceAssigner);
        sliceAssigner->SetWindowEnd(true);
    } else if (parsedJson.contains("sliceEndIndex")) { // todo GlobalWindowAgg should be AbstractSlicedSliceAssigner
        LOG("sliceEndIndex in assigner");
        sliceAssigner = new WindowedSliceAssigner(parsedJson["sliceEndIndex"].get<int>(), sliceAssigner);
        sliceAssigner->SetSliceEnd(true);
    } else {
        LOG("sliceEndIndex or windowEndIndex not in assigner");
    }
    return sliceAssigner;
}

CumulativeSliceAssigner* AssignerAtt::CreateCumlativeSliceAssigner(
    const nlohmann::json& parsedJson, int rowtimeIndexVal, omnistream::ZoneId* zonePtr)
{
    if (!parsedJson.contains("maxSize") || !parsedJson.contains("step")) {
        THROW_LOGIC_EXCEPTION("maxSize or step must be specified");
    }
    auto maxSizeNum = parsedJson["maxSize"].get<int64_t>();
    auto stepNum = parsedJson["step"].get<int64_t>();
    CumulativeSliceAssigner* sliceAssigner =
        SliceAssigners::SliceAssigners::cumulative(rowtimeIndexVal, zonePtr, maxSizeNum, stepNum);
    if (parsedJson.contains("windowOffset")) {
        auto offsetNum = parsedJson["windowOffset"].get<int64_t>();
        sliceAssigner = dynamic_cast<CumulativeSliceAssigner*>(sliceAssigner)->withOffset(offsetNum);
    }
    return sliceAssigner;
}

TumblingSliceAssigner* AssignerAtt::CreateTumbleSliceAssigner(
    const nlohmann::json& parsedJson, int rowtimeIndexVal, omnistream::ZoneId* zonePtr)
{
    if (!parsedJson.contains("windowSize")) {
        THROW_LOGIC_EXCEPTION("windowSize must be specified");
    }
    auto sizeNum = parsedJson["windowSize"].get<int64_t>();
    TumblingSliceAssigner* sliceAssigner = SliceAssigners::SliceAssigners::tumbling(rowtimeIndexVal, zonePtr, sizeNum);
    if (parsedJson.contains("windowOffset")) {
        auto offsetNum = parsedJson["windowOffset"].get<int64_t>();
        sliceAssigner = dynamic_cast<TumblingSliceAssigner*>(sliceAssigner)->withOffset(offsetNum);
    }
    return sliceAssigner;
}

HoppingSliceAssigner* AssignerAtt::CreateHopSliceAssigner(
    const nlohmann::json& parsedJson, int rowtimeIndexVal, omnistream::ZoneId* zonePtr)
{
    if (!parsedJson.contains("windowSize") || !parsedJson.contains("windowSlide")) {
        THROW_LOGIC_EXCEPTION("windowSize or windowSlide must be specified");
    }
    auto sizeNum = parsedJson["windowSize"].get<int64_t>();
    auto slideNum = parsedJson["windowSlide"].get<int64_t>();
    HoppingSliceAssigner* sliceAssigner = SliceAssigners::hopping(rowtimeIndexVal, zonePtr, sizeNum, slideNum);
    if (parsedJson.contains("windowOffset")) {
        auto offsetNum = parsedJson["windowOffset"].get<int64_t>();
        sliceAssigner = sliceAssigner->withOffset(offsetNum);
    }
    return sliceAssigner;
}
