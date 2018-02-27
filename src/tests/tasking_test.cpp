

#include <gtest/gtest.h>
#include <tasking/tasking.h>
#include <chrono>
#include <thread>

SMILE_NS_BEGIN

void testFunction( void* arg ) {
  int32_t* result = reinterpret_cast<int32_t*>(arg);
  *result=getCurrentThreadId();
}

/**
 * Creates one task per thread, where each task stores its therad id in the
 * result array
 * */
TEST(FileStorageTest, FileStorageOpen) {

  uint32_t numThreads = 4;

  int32_t* result = new int32_t[numThreads];

  // Starts Thread Pool
  startThreadPool(numThreads);

  SyncCounter syncCounter;
  for(int i = 0; i < numThreads; ++i) {
    executeTaskAsync(i, {testFunction, &result[i]}, &syncCounter);
    executeTaskAsync(i, {testFunction, &result[i]}, &syncCounter);
  }

  // Synchronize with the running tasks
  syncCounter.join();

  // Stops thread pool
  stopThreadPool();

  // Checks the results
  for(int i = 0; i < numThreads; ++i) {
    ASSERT_TRUE(result[i] == i);
  }

  delete [] result;
}

SMILE_NS_END

int main(int argc, char* argv[]){
  ::testing::InitGoogleTest(&argc,argv);
  int ret = RUN_ALL_TESTS();
  return ret;
}

