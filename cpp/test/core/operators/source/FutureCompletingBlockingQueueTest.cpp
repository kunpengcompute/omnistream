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

////
//// Created by xxx on 2025/2/10.
////
//
//// 测试队列的构造函数
// TEST(FutureCompletingBlockingQueueTest, ConstructorTest) {
//     FutureCompletingBlockingQueue<int*> queue(5);
//     EXPECT_EQ(queue.size(), 0);
//     EXPECT_EQ(queue.remainingCapacity(), 5);
//     EXPECT_TRUE(queue.isEmpty());
// }
//
//// 测试入队和出队操作
// TEST(FutureCompletingBlockingQueueTest, EnqueueDequeueTest) {
//     FutureCompletingBlockingQueue<int*> queue(5);
//     int* element = new int(10);
//     queue.put(0, element);
//     EXPECT_EQ(queue.size(), 1);
//     EXPECT_EQ(queue.remainingCapacity(), 4);
//     EXPECT_FALSE(queue.isEmpty());
//
//     int* dequeuedElement = queue.poll();
//     EXPECT_EQ(*dequeuedElement, 10);
//     EXPECT_EQ(queue.size(), 0);
//     EXPECT_EQ(queue.remainingCapacity(), 5);
//     EXPECT_TRUE(queue.isEmpty());
//
//     delete dequeuedElement; // 释放内存
// }
//
//// 测试队列满时 put 阻塞，poll 后唤醒的场景
// TEST(FutureCompletingBlockingQueueTest, BlockingPutAndWakeUpTest) {
//     // 创建一个容量为 1 的队列
//     FutureCompletingBlockingQueue<int*> queue(1);
//     std::atomic<bool> putCompleted(false);
//     std::atomic<bool> pollDone(false);
//
//     // 创建元素
//     int* initialElement = new int(20);
//     int* element = new int(10);
//
//     // 主线程先将一个元素放入队列，使队列满
//     bool initialPutResult = queue.put(0, initialElement);
//     EXPECT_TRUE(initialPutResult);
//
//     // 启动一个线程进行 put 操作，此时队列已满会阻塞
//     std::thread putThread([&]() {
//         bool result = queue.put(0, element);
//         putCompleted = result;
//     });
//
//     // 主线程等待一段时间，确保 put 线程已经阻塞
//     std::this_thread::sleep_for(std::chrono::milliseconds(100));
//     EXPECT_FALSE(putCompleted);
//
//     // 启动一个线程进行 poll 操作
//     std::thread pollThread([&]() {
//         int* dequeuedElement = queue.poll();
//         if (dequeuedElement) {
//             delete dequeuedElement;
//         }
//         pollDone = true;
//     });
//
//     // 等待 poll 线程完成
//     pollThread.join();
//     EXPECT_TRUE(pollDone);
//
//     // 等待 put 线程完成
//     putThread.join();
//     EXPECT_TRUE(putCompleted);
//
//// 清理内存
//    int* dequeuedElement = queue.poll();
//    if (dequeuedElement) {
//        delete dequeuedElement;
//    }
//}
