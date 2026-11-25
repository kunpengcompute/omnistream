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

#ifndef FLINK_TNEL_MAPVALUE_H
#define FLINK_TNEL_MAPVALUE_H

#include <typeinfo>
#include <string>
#include <emhash7.hpp>
#include <stdexcept>
#include "../io/IOReadableWritable.h"

template <typename K, typename V>
class MapValue : public IOReadableWritable {
public:
    void write(DataOutputSerializer& out) override;

    void read(DataInputView& in) override;

private:
    emhash7::HashMap<K, V> map;
};

template <typename K, typename V>
inline void MapValue<K, V>::read(DataInputView& in)
{
    int size = in.readInt();
    map.clear();

    //     //todo: only work for int type, will improve later
    //     if constexpr (std::is_same_v<K, int> || std::is_same_v<V, int>)  {
    //             cowMap[key] = value;
    //         }
    //     }

    // }
}

template <typename K, typename V>
inline void MapValue<K, V>::write(DataOutputSerializer& out)
{
    out.writeInt(map.size());
}

#endif
