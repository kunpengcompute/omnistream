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

#include "CompletableFuture.h"
#include <iostream>
#include <chrono>
#include <vector>
#include <memory>

using namespace omnistream;

// Sample task that sleeps for a specified duration
class SleepTask : public Runnable {
public:
    SleepTask(const std::string& name, int timeMs) : sleepTimeMs(timeMs), taskName(name)
    {
    }

    void run() override
    {
        std::cout << "Starting task: " << taskName << std::endl;
        std::this_thread::sleep_for(std::chrono::milliseconds(sleepTimeMs));
        std::cout << "Completed task: " << taskName << std::endl;
    }

private:
    int sleepTimeMs;
    std::string taskName;
};

// Task that prints a message
class PrintTask : public Runnable {
public:
    explicit PrintTask(const std::string& msg) : message(msg)
    {
    }

    void run() override
    {
        std::cout << "Message: " << message << std::endl;
    }

private:
    std::string message;
};

// Task that throws an exception
class ExceptionTask : public Runnable {
public:
    void run() override
    {
        std::cout << "About to throw an exception..." << std::endl;
        throw std::runtime_error("Intentional exception");
    }
};

int to_be_main()
{
    std::cout << "CompletableFuture Demo" << std::endl;

    // Example 1: Basic task execution
    std::cout << "\n=== Example 1: Basic runAs ===" << std::endl;
    auto task1 = std::make_shared<SleepTask>("Task-1", 2000);
    auto future1 = std::make_shared<CompletableFuture>();
    future1->runAs(task1);
    std::cout << "Waiting for task1 to complete..." << std::endl;
    future1->get();
    std::cout << "Task1 completed, state: " << static_cast<int>(future1->getState()) << std::endl;

    // Example 2: Run asynchronously
    std::cout << "\n=== Example 2: Run Async ===" << std::endl;
    auto task2 = std::make_shared<SleepTask>("Task-2", 1000);
    auto future2 = CompletableFuture::runAsync(task2);
    std::cout << "Task2 started asynchronously" << std::endl;
    std::cout << "Doing other work while task2 runs..." << std::endl;
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    std::cout << "More work..." << std::endl;
    future2->get();
    std::cout << "Task2 completed" << std::endl;

    // Example 3: Chain tasks
    std::cout << "\n=== Example 3: thenRun ===" << std::endl;
    auto task3a = std::make_shared<SleepTask>("Task-3A", 1500);
    auto task3b = std::make_shared<PrintTask>("This is the follow-up task 3B");

    auto future3 = CompletableFuture::runAsync(task3a)->thenRun(task3b);

    std::cout << "Waiting for chained tasks to complete..." << std::endl;
    future3->get();
    std::cout << "All chained tasks completed" << std::endl;

    // Example 4: Handle exceptions
    std::cout << "\n=== Example 4: Exception Handling ===" << std::endl;
    auto exceptionTask = std::make_shared<ExceptionTask>();
    auto future4 = CompletableFuture::runAsync(exceptionTask);

    try {
        future4->get();
    } catch (const std::exception& e) {
        std::cout << "Caught exception: " << e.what() << std::endl;
    }

    // Example 5: allOf and anyOf
    std::cout << "\n=== Example 5: allOf and anyOf ===" << std::endl;
    auto taskA = std::make_shared<SleepTask>("Task-A", 1000);
    auto taskB = std::make_shared<SleepTask>("Task-B", 2000);
    auto taskC = std::make_shared<SleepTask>("Task-C", 3000);

    auto futureA = CompletableFuture::runAsync(taskA);
    auto futureB = CompletableFuture::runAsync(taskB);
    auto futureC = CompletableFuture::runAsync(taskC);

    std::vector<std::shared_ptr<CompletableFuture>> allFutures = {futureA, futureB, futureC};

    // Wait for any task to complete
    std::cout << "Waiting for any task to complete..." << std::endl;
    auto anyFuture = CompletableFuture::anyOf(allFutures);
    anyFuture->get();
    std::cout << "At least one task is complete" << std::endl;

    // Wait for all tasks to complete
    std::cout << "Waiting for all tasks to complete..." << std::endl;
    auto allFuture = CompletableFuture::allOf(allFutures);
    allFuture->get();
    std::cout << "All tasks are complete" << std::endl;

    // Example 6: Timeout
    std::cout << "\n=== Example 6: Timeout ===" << std::endl;
    auto longTask = std::make_shared<SleepTask>("Long-Task", 5000);
    auto longFuture = CompletableFuture::runAsync(longTask);

    std::cout << "Waiting for task with timeout..." << std::endl;
    bool completed = longFuture->get(2000);
    if (completed) {
        std::cout << "Task completed within timeout" << std::endl;
    } else {
        std::cout << "Task did not complete within timeout" << std::endl;
    }

    std::cout << "Waiting for task to finally complete..." << std::endl;
    longFuture->get();
    std::cout << "Long task finally completed" << std::endl;

    return 0;
}
