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

#ifndef OMNISTREAM_MUTIPLEINPUTSELECTIONHANDLER_H
#define OMNISTREAM_MUTIPLEINPUTSELECTIONHANDLER_H

#include <stdexcept>
#include "DataInputStatus.h"
#include "common.h"

namespace omnistream {

class MutipleInputSelectionHandler {
public:
    explicit MutipleInputSelectionHandler(int inputCount)
        : dataFinishedButNotPartition(0),
          operatingMode(NO_INPUT_SELECTABLE)
    {
        allSelectedMask = (1L << inputCount) - 1;
        availableInputsMask = allSelectedMask;
        notFinishedInputsMask = allSelectedMask;
    }

    DataInputStatus updateStatusAndSelection(DataInputStatus inputStatus, int inputIndex)
    {
        if (inputStatus == DataInputStatus::END_OF_RECOVERY) {
            return DataInputStatus::END_OF_RECOVERY;
        }
        if (inputStatus == DataInputStatus::INNER_RECOVER) {
            return DataInputStatus::INNER_RECOVER;
        }
        switch (inputStatus) {
            case DataInputStatus::MORE_AVAILABLE:
                nextSelection();
                if (!checkBitMask(availableInputsMask, inputIndex)) {
                    THROW_RUNTIME_ERROR("input " << std::to_string(inputIndex) << " is not available");
                }
                return DataInputStatus::MORE_AVAILABLE;
            case DataInputStatus::NOTHING_AVAILABLE:
                availableInputsMask = unsetBitMask(availableInputsMask, inputIndex);
                break;
            case DataInputStatus::END_OF_DATA:
                dataFinishedButNotPartition = setBitMask(dataFinishedButNotPartition, inputIndex);
                updateModeOnEndOfData();
                break;
            case DataInputStatus::END_OF_INPUT:
                dataFinishedButNotPartition = unsetBitMask(dataFinishedButNotPartition, inputIndex);
                notFinishedInputsMask = unsetBitMask(notFinishedInputsMask, inputIndex);
                break;
            default: throw std::runtime_error("Unsupported inputStatus");
        }
        nextSelection();
        return calculateOverallStatus(inputStatus);
    }

    void updateModeOnEndOfData()
    {
        uint64_t udataFinishedButNotPartition = static_cast<uint64_t>(dataFinishedButNotPartition);
        uint64_t unotFinishedInputsMask = static_cast<uint64_t>(notFinishedInputsMask);
        uint64_t uallSelectedMask = static_cast<uint64_t>(allSelectedMask);
        bool allDataInputsFinished =
            ((udataFinishedButNotPartition | ~unotFinishedInputsMask) & uallSelectedMask) == uallSelectedMask;
        if (allDataInputsFinished) {
            operatingMode = OperatingMode::ALL_DATA_INPUTS_FINISHED;
        } else if (operatingMode == OperatingMode::INPUT_SELECTABLE_PRESENT_NO_DATA_INPUTS_FINISHED) {
            operatingMode = OperatingMode::INPUT_SELECTABLE_PRESENT_SOME_DATA_INPUTS_FINISHED;
        }
    }

    DataInputStatus calculateOverallStatus(DataInputStatus updatedStatus)
    {
        if (areAllInputsFinished()) {
            return DataInputStatus::END_OF_INPUT;
        }

        if (updatedStatus == DataInputStatus::END_OF_DATA && operatingMode == OperatingMode::ALL_DATA_INPUTS_FINISHED) {
            return DataInputStatus::END_OF_DATA;
        }

        if (isAnyInputAvailable()) {
            return DataInputStatus::MORE_AVAILABLE;
        } else {
            long selectedNotFinishedInputMask = static_cast<int64_t>(
                static_cast<uint64_t>(selectedInputsMask) & static_cast<uint64_t>(notFinishedInputsMask));
            if (selectedNotFinishedInputMask == 0) {
                throw std::runtime_error("Can not make a progress: all selected inputs are already finished");
            }
            return DataInputStatus::NOTHING_AVAILABLE;
        }
    }

    inline bool areAllInputsFinished()
    {
        return notFinishedInputsMask == 0;
    }

    inline bool isAnyInputAvailable() const
    {
        uint64_t uselectedInputsMask = static_cast<uint64_t>(selectedInputsMask);
        uint64_t unotFinishedInputsMask = static_cast<uint64_t>(notFinishedInputsMask);
        uint64_t uavailableInputsMask = static_cast<uint64_t>(availableInputsMask);
        return (uselectedInputsMask & uavailableInputsMask & unotFinishedInputsMask) != 0;
    }

    inline bool isInputSelected(int inputIndex)
    {
        return checkBitMask(selectedInputsMask, inputIndex);
    }

    inline bool isInputFinished(int inputIndex)
    {
        return !checkBitMask(notFinishedInputsMask, inputIndex);
    }

    void nextSelection()
    {
        switch (operatingMode) {
            case NO_INPUT_SELECTABLE:
            case ALL_DATA_INPUTS_FINISHED: selectedInputsMask = -1; break;
            case INPUT_SELECTABLE_PRESENT_NO_DATA_INPUTS_FINISHED: break;
            case INPUT_SELECTABLE_PRESENT_SOME_DATA_INPUTS_FINISHED: break;
        }
    }

    void SetAvailableInput(int inputIndex)
    {
        availableInputsMask = setBitMask(availableInputsMask, inputIndex);
    }

    bool ShouldSetAvailableForAnotherInput()
    {
        uint64_t uselectedInputsMask = static_cast<uint64_t>(selectedInputsMask);
        uint64_t uallSelectedMask = static_cast<uint64_t>(allSelectedMask);
        uint64_t uavailableInputsMask = static_cast<uint64_t>(availableInputsMask);
        return (uselectedInputsMask & uallSelectedMask & ~uavailableInputsMask) != 0;
    }

    inline long setBitMask(long mask, int inputIndex)
    {
        uint64_t umask = static_cast<uint64_t>(mask);
        return umask | 1L << inputIndex;
    }

    inline long unsetBitMask(long mask, int inputIndex)
    {
        uint64_t umask = static_cast<uint64_t>(mask);
        return umask & ~(1L << inputIndex);
    }

    inline bool checkBitMask(long mask, int inputIndex)
    {
        uint64_t umask = static_cast<uint64_t>(mask);
        return (umask & (1L << inputIndex)) != 0;
    }

    int selectNextInputIndex(int lastReadInputIndex);

private:
    long selectedInputsMask = -1;

    long allSelectedMask;

    long availableInputsMask;

    long notFinishedInputsMask;

    long dataFinishedButNotPartition;

    enum OperatingMode {
        NO_INPUT_SELECTABLE,
        INPUT_SELECTABLE_PRESENT_NO_DATA_INPUTS_FINISHED,
        INPUT_SELECTABLE_PRESENT_SOME_DATA_INPUTS_FINISHED,
        ALL_DATA_INPUTS_FINISHED
    };

    OperatingMode operatingMode;
};

} // namespace omnistream

#endif
