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

#ifndef FLINK_TNEL_STATEOBJECT_H
#define FLINK_TNEL_STATEOBJECT_H
#include <string>
#include <memory>
#include <nlohmann/json.hpp>

#include <string>

/**
 * Base class for all handles that represent checkpointed state in some form.
 * The object may hold the (small) state directly, contain a file path (with the state in the file),
 * or include metadata to access state stored in an external database.
 *
 * State objects define how to discard the state (`discardState()`) and how to access the
 * size of the state (`getStateSize()`).
 *
 * State objects are transported via RPC between the JobManager and TaskManager and must
 * have serializer to support that.
 *
 * Some state objects are stored in checkpoint/savepoint metadata. For long-term compatibility,
 * they are serialized using custom serializers rather than built-in serialization.
 */

class StateObject {
public:
    virtual ~StateObject() = default;

    /**
     * Discards the state referred to and solemnly owned by this handle, to free up resources in the
     * persistent storage. This method is called when the state represented by this object will not
     * be used any more.
     */
    virtual void DiscardState() = 0;

    /**
     * Returns the size of the state in bytes. If the size is not known, this method should return
     * 0.
     *
     * The values produced by this method are only used for informational purposes and for
     * metrics/monitoring. If this method returns wrong values, the checkpoints and recovery will
     * still behave correctly. However, efficiency may be impacted (wrong space pre-allocation) and
     * functionality that depends on metrics (like monitoring) will be impacted.
     *
     * Note for implementors: This method should not perform any I/O operations while obtaining
     * the state size (hence it does not declare throwing an exception). Instead, the
     * state size should be stored in the state object, or should be computable from the state
     * stored in this object. The reason is that this method is called frequently by several parts
     * of the checkpointing and issuing I/O requests from this method accumulates a heavy I/O load
     * on the storage system at higher scale.
     *
     * @return Size of the state in bytes.
     */
    virtual long GetStateSize() const = 0;

    virtual std::string ToString() const = 0;
};
#endif // FLINK_TNEL_STATEOBJECT_H
