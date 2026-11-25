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

// metric_manager.cpp
#include "MetricManager.h"
#include <cerrno>
#include <cstring>
#include <sstream>

namespace omnistream {
namespace {

constexpr size_t MIN_SHARED_MEMORY_SIZE = sizeof(uint64_t);

// 校验并收紧已有 shm，避免映射属主、权限或大小不符合预期的对象。
bool ValidateExistingSharedMemory(int fd, const std::string& key, size_t expectedSize)
{
    struct stat st{};
    if (fstat(fd, &st) == -1) {
        std::stringstream ss_;
        ss_ << "fstat failed for key: " << key << ", error: " << std::strerror(errno);
        GErrorLog(ss_.str());
        return false;
    }
    if (st.st_uid != geteuid()) {
        std::stringstream ss_;
        ss_ << "shared memory owner mismatch for key: " << key << ", owner uid=" << st.st_uid << ", euid=" << geteuid();
        GErrorLog(ss_.str());
        return false;
    }

    const mode_t permissions = st.st_mode & (S_IRWXU | S_IRWXG | S_IRWXO);
    if (permissions != MetricManager::sharedMemoryFDMode && fchmod(fd, MetricManager::sharedMemoryFDMode) == -1) {
        std::stringstream ss_;
        ss_ << "failed to restrict shared memory permissions for key: " << key << ", error: " << std::strerror(errno);
        GErrorLog(ss_.str());
        return false;
    }

    if (st.st_size < 0 || static_cast<size_t>(st.st_size) != expectedSize) {
        std::stringstream ss_;
        ss_ << "shared memory size mismatch for key: " << key << ", actual size=" << st.st_size
            << ", expected size=" << expectedSize;
        GErrorLog(ss_.str());
        return false;
    }
    return true;
}

void CloseSharedMemoryFd(int& fd)
{
    if (fd != -1) {
        close(fd);
        fd = -1;
    }
}

} // namespace

const char* MetricManager::sharedMemoryKeyPrefix = "OMNI_SHM_METRIC";
long MetricManager::omniStreamTaskProcessInputID = 1;
std::unique_ptr<MetricManager> MetricManager::instance;
std::mutex MetricManager::singletonMutex;
const int MetricManager::sharedMemoryFDMode = S_IRUSR | S_IWUSR;

MetricManager::MetricManager(const std::string& monitorKey) : monitorKey(monitorKey)
{
}

MetricManager::~MetricManager()
{
    if (sharedMemoryPtr != nullptr) {
        if (munmap(sharedMemoryPtr, sharedMemorySize) == -1) {
            std::stringstream ss_;
            ss_ << "munmap failed for key: " << sharedMemoryKey;
            GErrorLog(ss_.str());
        }
    }
    if (sharedMemoryFd != -1) {
        if (close(sharedMemoryFd) == -1) {
            std::stringstream ss_;
            ss_ << "close failed for key: " << sharedMemoryKey;
            GErrorLog(ss_.str());
        }
        if (shm_unlink(sharedMemoryKey.c_str()) == -1) {
            std::stringstream ss_;
            ss_ << "shm_unlink failed for key: " << sharedMemoryKey;
            GErrorLog(ss_.str());
        }
    }
}

bool MetricManager::Setup(size_t size)
{
    if (size < MIN_SHARED_MEMORY_SIZE) {
        std::stringstream ss_;
        ss_ << "shared memory size is too small: " << size << ", minimum size=" << MIN_SHARED_MEMORY_SIZE;
        GErrorLog(ss_.str());
        return false;
    }

    pid_t pid = getpid();
    std::stringstream ss_;
    ss_ << "/" << monitorKey << "_" << pid;
    sharedMemoryKey = ss_.str();
    sharedMemorySize = size;

    // 先尝试打开已有对象；不存在再独占创建，避免 TOCTOU 挂到他人对象。
    sharedMemoryFd = shm_open(sharedMemoryKey.c_str(), O_RDWR, 0);
    if (sharedMemoryFd == -1) {
        if (errno == ENOENT) {
            return CreateSharedMemory(size);
        }
        std::stringstream ss_;
        ss_ << "shm_open failed for key: " << sharedMemoryKey << ", error: " << std::strerror(errno);
        GErrorLog(ss_.str());
        return false;
    }

    return AttachSharedMemory(size);
}

bool MetricManager::AttachSharedMemory(size_t size)
{
    if (!ValidateExistingSharedMemory(sharedMemoryFd, sharedMemoryKey, size)) {
        CloseSharedMemoryFd(sharedMemoryFd);
        return false;
    }

    sharedMemoryPtr = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, sharedMemoryFd, 0);
    if (sharedMemoryPtr == MAP_FAILED) {
        std::stringstream ss_;
        ss_ << "mmap failed for existing key: " << sharedMemoryKey << ", error: " << std::strerror(errno);
        GErrorLog(ss_.str());
        sharedMemoryPtr = nullptr;
        CloseSharedMemoryFd(sharedMemoryFd);
        return false;
    }

    sharedMemorySize = size;
    DisableMonitoring();
    INFO_RELEASE("Successfully hooked to existing shared memory: " << sharedMemoryKey);
    return true;
}

void* MetricManager::GetDataPtr() const
{
    return sharedMemoryPtr;
}

size_t MetricManager::GetSize() const
{
    return sharedMemorySize;
}

void MetricManager::EnableMonitoring()
{
    *static_cast<uint8_t*>(sharedMemoryPtr) = 1;
}

void MetricManager::DisableMonitoring()
{
    *static_cast<uint8_t*>(sharedMemoryPtr) = 1;
}

bool MetricManager::CreateSharedMemory(size_t size)
{
    // O_EXCL：创建窗口内若已被他人抢先创建则失败，不挂接不可信对象。
    sharedMemoryFd = shm_open(sharedMemoryKey.c_str(), O_CREAT | O_EXCL | O_RDWR, sharedMemoryFDMode);
    if (sharedMemoryFd == -1) {
        if (errno == EEXIST) {
            // 另一初始化者在 open/create 窗口内完成创建时，重新打开并执行同样的安全校验。
            sharedMemoryFd = shm_open(sharedMemoryKey.c_str(), O_RDWR, 0);
            if (sharedMemoryFd != -1) {
                return AttachSharedMemory(size);
            }
        }
        std::stringstream ss_;
        ss_ << "shm_open (creation) failed for key: " << sharedMemoryKey << ", error: " << std::strerror(errno);
        GErrorLog(ss_.str());
        return false;
    }

    auto failAndUnlink = [this](const char* what) {
        std::stringstream ss_;
        ss_ << what << " failed for key: " << sharedMemoryKey << ", error: " << std::strerror(errno);
        GErrorLog(ss_.str());
        CloseSharedMemoryFd(sharedMemoryFd);
        shm_unlink(sharedMemoryKey.c_str());
        return false;
    };

    // 显式收紧权限，不依赖进程 umask。
    if (fchmod(sharedMemoryFd, sharedMemoryFDMode) == -1) {
        return failAndUnlink("fchmod");
    }

    if (ftruncate(sharedMemoryFd, size) == -1) {
        return failAndUnlink("ftruncate");
    }

    sharedMemoryPtr = mmap(nullptr, sharedMemorySize, PROT_READ | PROT_WRITE, MAP_SHARED, sharedMemoryFd, 0);
    if (sharedMemoryPtr == MAP_FAILED) {
        sharedMemoryPtr = nullptr;
        return failAndUnlink("mmap");
    }

    LOG("Successfully created and mapped shared memory: " << sharedMemoryKey << " with size: " << sharedMemorySize);
    return true;
}

} // namespace omnistream
