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

#ifndef OMNISTREAM_NEXMARKUTILS_H
#define OMNISTREAM_NEXMARKUTILS_H

#include <iostream>
#include <string>
#include <stdexcept>
#include <vector>
#include <cmath>

class NexmarkUtils {
public:
    class RateUnit {
    public:
        // Static members for RateUnit.
        static const RateUnit PER_SECOND;
        static const RateUnit PER_MINUTE;

    private:
        // Number of microseconds per unit.
        long usPerUnit;

        // Private constructor to initialize RateUnit.
        explicit RateUnit(long usPerUnit) : usPerUnit(usPerUnit)
        {
        }

        // Grant NexmarkUtils access if needed.
        friend class NexmarkUtils;
    };

    //
    // Shape of event rate.
    //
    class RateShape {
    public:
        // Return delay between steps, in seconds, for result of interEventDelayUs, so as to
        // cycle through the entire sequence every ratePeriodSec.
        int stepLengthSec(int ratePeriodSec) const
        {
            int n = 0;
            switch (type) {
                case SQUARE: n = 2; break;
                case SINE: n = N; break;
            }
            return (ratePeriodSec + n - 1) / n;
        }

        // Public enum values for RateShape.
        enum Type {
            SQUARE,
            SINE
        };

        // Static instances for the shapes.
        static const RateShape SQUARE_SHAPE;
        static const RateShape SINE_SHAPE;

    public:
        Type type;
        // Number of steps used to approximate sine wave.
        static const int N = 10;

        // Private constructor.
        RateShape(Type type) : type(type)
        {
        }
    };
};

#endif // OMNISTREAM_NEXMARKUTILS_H
