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

#ifndef OMNISTREAM_CLOSEABLEREGISTRY_H
#define OMNISTREAM_CLOSEABLEREGISTRY_H

#include <list>
#include <map>
#include <memory>
#include <utility>
#include <algorithm>
#include <stdexcept>

class Closeable {
public:
    virtual void close() = 0;
    virtual ~Closeable() = default;
};

template <typename T, typename M, typename L>
class AbstractAutoCloseableRegistry {
public:
    virtual ~AbstractAutoCloseableRegistry() = default;

    void registerCloseable(T closeable)
    {
        if (closed) {
            closeable->close();
            throw std::runtime_error("Cannot register to closed registry");
        }
        doRegister(closeable, closeableMap);
    }

    bool unregisterCloseable(T closeable)
    {
        if (closed) {
            return false;
        }
        return doUnRegister(closeable, closeableMap);
    }

    void close()
    {
        if (!closed) {
            closed = true;
            std::list<T> toClose;
            for (const auto& entry : closeableMap) {
                toClose.push_back(entry.first);
            }
            doClose(toClose);
        }
    }

protected:
    explicit AbstractAutoCloseableRegistry(M map) : closeableMap(std::move(map))
    {
    }
    virtual void doRegister(T closeable, M& map) = 0;
    virtual bool doUnRegister(T closeable, M& map) = 0;
    virtual void doClose(std::list<T>& toClose) = 0;

    M closeableMap;
    bool closed = false;
};

class CloseableRegistry : public AbstractAutoCloseableRegistry<
                              std::shared_ptr<Closeable>,
                              std::map<std::shared_ptr<Closeable>, void*>,
                              void> {
public:
    CloseableRegistry() : AbstractAutoCloseableRegistry(std::map<std::shared_ptr<Closeable>, void*>())
    {
    }

protected:
    void doRegister(
        std::shared_ptr<Closeable> closeable, std::map<std::shared_ptr<Closeable>, void*>& closeableMap) override
    {
        closeableMap[closeable] = DUMMY;
    }

    bool doUnRegister(
        std::shared_ptr<Closeable> closeable, std::map<std::shared_ptr<Closeable>, void*>& closeableMap) override
    {
        return closeableMap.erase(closeable) > 0;
    }

    void doClose(std::list<std::shared_ptr<Closeable>>& toClose) override
    {
        std::reverse(toClose.begin(), toClose.end());
        for (const auto& closeable : toClose) {
            try {
                closeable->close();
            } catch (const std::exception& e) {
                throw std::runtime_error("doClose failed: " + std::string(e.what()));
            }
        }
    }

private:
    static inline void* DUMMY = nullptr;
};

#endif // OMNISTREAM_CLOSEABLEREGISTRY_H
