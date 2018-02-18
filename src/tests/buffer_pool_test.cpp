#include <gtest/gtest.h>
#include <memory/buffer_pool.h>

SMILE_NS_BEGIN

/**
 * Tests Buffer Pool allocations. We first reserve space in the FileStorage for 8 pages and
 * declare a 4-slot Buffer Pool. Then, we ask for allocating the 4 first ones into the pool.
 * With the pool full, we try to allocate space for the other 4 pages and check that the 
 * slots are being reused. Finally, a couple of buffers are pinned to see that the Clock
 * Sweep Algorithm is behaving as expected.
 */
TEST(BufferPoolTest, BufferPoolAlloc) {
  FileStorage fileStorage;
  ASSERT_TRUE(fileStorage.create("./test.db", FileStorageConfig{64}, true) == ErrorCode::E_NO_ERROR);

  BufferPool bufferPool(&fileStorage, 4);

  extentId_t eId;
  ASSERT_TRUE(fileStorage.reserve(8,eId) == ErrorCode::E_NO_ERROR);

  transactionId_t tId = 1;
  bufferId_t bId = bufferPool.alloc(tId,1);
  ASSERT_TRUE(bId == 0);
  bId = bufferPool.alloc(tId,2);
  ASSERT_TRUE(bId == 1);
  bId = bufferPool.alloc(tId,3);
  ASSERT_TRUE(bId == 2);
  bId = bufferPool.alloc(tId,4);
  ASSERT_TRUE(bId == 3);
  bId = bufferPool.alloc(tId,5);
  ASSERT_TRUE(bId == 0);
  bId = bufferPool.alloc(tId,6);
  ASSERT_TRUE(bId == 1);
  bId = bufferPool.alloc(tId,7);
  ASSERT_TRUE(bId == 2);
  bId = bufferPool.alloc(tId,8);
  ASSERT_TRUE(bId == 3);

  Buffer buffer = bufferPool.pin(0,tId);
  buffer = bufferPool.pin(1,tId);
  bId = bufferPool.alloc(tId,1);
  ASSERT_TRUE(bId == 2);
}

/**
 * Tests reading and writing a buffer. A page is allocated to the buffer pool and then it is 
 * pinned. Some data is written into it by using a handler and finally we read it to check that
 * everything went well.
 */
TEST(BufferPoolTest, BufferPoolReadWrite) {
  FileStorage fileStorage;
  ASSERT_TRUE(fileStorage.create("./test.db", FileStorageConfig{64}, true) == ErrorCode::E_NO_ERROR);

  BufferPool bufferPool(&fileStorage, 4);

  extentId_t eId;
  ASSERT_TRUE(fileStorage.reserve(8,eId) == ErrorCode::E_NO_ERROR);

  transactionId_t tId = 1;
  bufferId_t bId = bufferPool.alloc(tId,1);
  ASSERT_TRUE(bId == 0);
  Buffer buffer = bufferPool.pin(0,tId);
  char dataW[] = "I am writing data";
  buffer.write(dataW, 17, 100);

  char dataR[17];
  buffer.read(dataR, 17, 100);
  for (int i = 0; i < 17; ++i)
  {
    ASSERT_TRUE(dataW[i] == dataR[i]);
  }
}

/**
 * Tests releasing a page from the buffer pool. A page is allocated and written, so it gets
 * dirty. Then, buffer pool is asked for release it, which makes it flush the modified
 * page to disk. Finally, the page is pinned again to check that the changes have been correctly
 * saved to permanent storage.
 */
TEST(BufferPoolTest, BufferPoolReadAfterRelease) {
  FileStorage fileStorage;
  ASSERT_TRUE(fileStorage.create("./test.db", FileStorageConfig{64}, true) == ErrorCode::E_NO_ERROR);

  BufferPool bufferPool(&fileStorage, 4);

  extentId_t eId;
  ASSERT_TRUE(fileStorage.reserve(8,eId) == ErrorCode::E_NO_ERROR);

  transactionId_t tId = 1;
  bufferId_t bId = bufferPool.alloc(tId,1);
  ASSERT_TRUE(bId == 0);
  Buffer buffer = bufferPool.pin(0,tId);
  char dataW[] = "I am writing data";
  buffer.write(dataW, 17, 100);
  
  bufferPool.release(0);

  bId = bufferPool.alloc(tId,1);
  ASSERT_TRUE(bId == 0);
  buffer = bufferPool.pin(0,tId);
  char dataR[17];
  buffer.read(dataR, 17, 100);

  for (int i = 0; i < 17; ++i)
  {
    ASSERT_TRUE(dataW[i] == dataR[i]);
  }
}

SMILE_NS_END

int main(int argc, char* argv[]){
  ::testing::InitGoogleTest(&argc,argv);
  int ret = RUN_ALL_TESTS();
  return ret;
}