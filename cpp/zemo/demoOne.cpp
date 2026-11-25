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

#include <iostream>
#include <atomic>
#include <thread>
#include <vector>
#include <chrono>

int main()
{
    std::atomic<bool> ready(false); // Initialize an atomic boolean to false

    // Worker thread function
    auto worker_thread_func = [&ready]() {
        bool expected = false; // Expected value for compare_exchange_strong
        bool desired = true;   // Desired value to set

        // Attempt to atomically change ready from false to true
        while (!ready.compare_exchange_strong(expected, desired)) {
            // compare_exchange_strong failed:
            // - The current value of ready was not equal to expected.
            // - expected is updated to the current value of ready.
            // - We retry the operation.

            // Optional: Add a small delay to prevent tight loops
            std::this_thread::sleep_for(std::chrono::milliseconds(1));

            expected = false; // Reset expected to the current value(which was updated by compare_exchange_strong)
        }

        std::cout << "Worker thread: Ready is now true." << std::endl;

        // Do some work...
        std::this_thread::sleep_for(std::chrono::seconds(2));
        std::cout << "Worker thread: Work completed." << std::endl;
    };

    // Main thread (initiator)
    std::thread worker(worker_thread_func);

    // Give the worker thread a moment to start
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Simulate some setup work in the main thread
    std::cout << "Main thread: Performing setup..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(1));
    std::cout << "Main thread: Setup complete." << std::endl;

    // Signal the worker thread to start its main work
    bool expected_main = false;
    bool desired_main = true;
    ready.compare_exchange_strong(expected_main, desired_main); // change ready to true.

    std::cout << "Main thread: Signaled worker thread." << std::endl;

    worker.join(); // Wait for the worker thread to finish

    std::cout << "Main thread: Worker thread finished." << std::endl;

    return 0;
}
