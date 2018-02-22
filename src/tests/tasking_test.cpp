

#include <gtest/gtest.h>
#include <tasking/tasking.h>
#include <chrono>
#include <thread>

SMILE_NS_BEGIN

uint32_t* finalNumbers;

void testFunction( void* id ) {
  uint32_t number = *(uint32_t*)id;
  finalNumbers[getCurrentThreadId()]+=number;
}

/**
 * Creates two tasks per thread, each of them accumulating a number into the
 * finalNumbers array.
 * */
TEST(FileStorageTest, FileStorageOpen) {

  uint32_t numThreads = 4;

  finalNumbers = new uint32_t[numThreads];
  uint32_t numbers[numThreads*2];
  startThreadPool(numThreads);
  for(int i = 0; i < numThreads; ++i) {
    numbers[i] = i;
    numbers[numThreads+i] = i;
    finalNumbers[i] = 0;
    executeTask({testFunction, &numbers[i]}, i);
    executeTask({testFunction, &numbers[numThreads+i]}, i);
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(1000));

  stopThreadPool();

  for(int i = 0; i < numThreads;++i) {
    ASSERT_TRUE(finalNumbers[i] == 2*i);
  }
  delete finalNumbers;
}

SMILE_NS_END

int main(int argc, char* argv[]){
  ::testing::InitGoogleTest(&argc,argv);
  int ret = RUN_ALL_TESTS();
  return ret;
}

