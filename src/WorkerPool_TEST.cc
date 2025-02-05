/*
 * Copyright (C) 2017 Open Source Robotics Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
*/

#include <gtest/gtest.h>

#include <atomic>
#include <thread>

#include "gz/common/Console.hh"
#include "gz/common/WorkerPool.hh"
#include "gz/utils/ExtraTestMacros.hh"

using namespace gz;

//////////////////////////////////////////////////
TEST(WorkerPool, OneWorkNoCallback)
{
  common::WorkerPool pool;
  int workSentinel = 0;

  pool.AddWork([&workSentinel] ()
      {
        workSentinel = 5;
      });
  EXPECT_TRUE(pool.WaitForResults());
  EXPECT_EQ(5, workSentinel);
}

//////////////////////////////////////////////////
TEST(WorkerPool, OneWorkWithCallback)
{
  common::WorkerPool pool;
  int workSentinel = 0;
  int cbSentinel = 0;

  pool.AddWork([&workSentinel] ()
      {
        workSentinel = 5;
      },
    [&cbSentinel] ()
      {
        cbSentinel = 10;
      });
  EXPECT_TRUE(pool.WaitForResults());
  EXPECT_EQ(5, workSentinel);
  EXPECT_EQ(10, cbSentinel);
}

//////////////////////////////////////////////////
TEST(WorkerPool, LotsOfWork)
{
  common::WorkerPool pool;
  std::atomic<int> workSentinel(0);
  std::atomic<int> cbSentinel(0);

  for (int i = 0; i < 1000; i++)
  {
    pool.AddWork([&workSentinel] ()
        {
          workSentinel += 1;
        },
      [&cbSentinel] ()
        {
          cbSentinel += 2;
        });
  }
  EXPECT_TRUE(pool.WaitForResults());
  EXPECT_EQ(1000, workSentinel);
  EXPECT_EQ(2000, cbSentinel);
}

//////////////////////////////////////////////////
TEST(WorkerPool, WaitWithTimeout)
{
  common::WorkerPool pool;
  int workSentinel = 0;
  pool.AddWork([&workSentinel] ()
      {
        workSentinel = 5;
      });
  EXPECT_TRUE(pool.WaitForResults(std::chrono::seconds(5)));
  EXPECT_EQ(5, workSentinel);
}

TEST(WorkerPool, WaitWithTimeoutThatTimesOut)
{
  common::WorkerPool pool;
  pool.AddWork([] ()
      {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
      });
  EXPECT_FALSE(pool.WaitForResults(std::chrono::milliseconds(1)));
}

TEST(WorkerPool, ThingsRunInParallel)
{
  const auto hc = std::thread::hardware_concurrency();
  if (2 > hc)
  {
    gzdbg << "Skipping the ThingsRunInParallel test because hardware "
           << "concurrency (" << hc << ") is too low (min: 2), making the test "
           << "less likely to succeed.\n";
    return;
  }

  common::WorkerPool pool;
  std::atomic<int> sentinel(0);
  pool.AddWork([&sentinel] ()
      {
        ++sentinel;
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
      });
  pool.AddWork([&sentinel] ()
      {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        ++sentinel;
      });
  bool result = pool.WaitForResults(std::chrono::milliseconds(90));
  EXPECT_TRUE(result);
  if (!result)
  {
    gzdbg << "WaitForResults failed" << std::endl;
  }
  EXPECT_EQ(2, sentinel);
}

TEST(WorkerPool, SafeDestruction)
{
  const auto hc = std::thread::hardware_concurrency();
  // using atomic to access from different threads
  std::atomic<common::WorkerPool*> poolPtr;
  poolPtr = new common::WorkerPool(hc);

  // should be enough to take all workers
  const auto tasks = hc * 2;

  std::atomic<unsigned int> sentinel{0};
  std::atomic<int> resultsReady{0};

  for (unsigned int i{}; i < tasks; ++i)
  {
    poolPtr.load()->AddWork([&sentinel] ()
      {
        // sleep long enough to create whole tasks and start destruction
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        ++sentinel;
      });
  }

  auto t1 = std::thread([&](){
    poolPtr.load()->WaitForResults();
    ++resultsReady;
  });
  auto t2 = std::thread([&](){
    poolPtr.load()->WaitForResults();
    ++resultsReady;
  });
  auto t3 = std::thread([&](){
    poolPtr.load()->WaitForResults();
    ++resultsReady;
  });

  EXPECT_FALSE(resultsReady);
  // this destructor call will be blocked until whole workers are joined
  // and whole threads who waiting will be notified
  delete poolPtr.load();
  EXPECT_EQ(resultsReady, 3);
  EXPECT_LT(sentinel, tasks);
  // on a very slow setup, like thread sanitizer EXPECT_GE(sentinel, hc) will
  // fail due to thread even dont start execution the task, so it's not added

  t1.join();
  t2.join();
  t3.join();
}
