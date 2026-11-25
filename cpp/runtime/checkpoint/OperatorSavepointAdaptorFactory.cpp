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

#include "OperatorSavepointAdaptorFactory.h"
#include "OperatorSavepointAdaptor.h"
#include "GroupAggSavepointAdaptor.h"
#include "StreamingJoinSavepointAdaptor.h"

namespace omnistream {
std::unique_ptr<OperatorSavepointAdaptor> OperatorSavepointAdaptorFactory::createAdaptor(FlinkSavepointAdaptorType type)
{
    switch (type) {
        case FlinkSavepointAdaptorType::GroupAggAdaptor: return std::make_unique<GroupAggSavepointAdaptor>();
        case FlinkSavepointAdaptorType::StreamingJoinNoUniqueKeyAdaptor:
        case FlinkSavepointAdaptorType::StreamingLeftOuterJoinNoUniqueKeyAdaptor:
            return std::make_unique<StreamingJoinSavepointAdaptor>(type);
        case FlinkSavepointAdaptorType::OmniIsCompatible:
        case FlinkSavepointAdaptorType::None:
        default: return nullptr;
    }
}
} // namespace omnistream
