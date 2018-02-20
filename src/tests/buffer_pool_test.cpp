#include <gtest/gtest.h>
#include <memory/buffer_pool.h>

SMILE_NS_BEGIN

/**
 * Tests Buffer Pool allocations. We first declare a 4-slot (256 KB) Buffer Pool (64 KB page
 * size). Then, we ask for allocating 4 pages into the pool and unpin them. With the pool full,
 * we try to allocate space for 4 other pages and check that the slots are being reused.
 * Finally, a couple of buffers are unpinned before allocating another page in order to see
 * that the Clock Sweep Algorithm is behaving as expected.
 */
TEST(BufferPoolTest, BufferPoolAlloc) {
  FileStorage fileStorage;
  ASSERT_TRUE(fileStorage.create("./test.db", FileStorageConfig{64}, true) == ErrorCode::E_NO_ERROR);
  BufferPool bufferPool(&fileStorage, BufferPoolConfig{256});
  BufferHandler bufferHandler1, bufferHandler2, bufferHandler3, bufferHandler4;

  ASSERT_TRUE(bufferPool.alloc(bufferHandler1) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferHandler1.m_bId == 0);
  ASSERT_TRUE(bufferPool.unpin(bufferHandler1.m_pId) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferPool.alloc(bufferHandler2) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferHandler2.m_bId == 1);
  ASSERT_TRUE(bufferPool.unpin(bufferHandler2.m_pId) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferPool.alloc(bufferHandler3) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferHandler3.m_bId == 2);
  ASSERT_TRUE(bufferPool.unpin(bufferHandler3.m_pId) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferPool.alloc(bufferHandler4) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferHandler4.m_bId == 3);
  ASSERT_TRUE(bufferPool.unpin(bufferHandler4.m_pId) == ErrorCode::E_NO_ERROR);

  ASSERT_TRUE(bufferPool.alloc(bufferHandler1) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferHandler1.m_bId == 0);
  ASSERT_TRUE(bufferPool.alloc(bufferHandler2) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferHandler2.m_bId == 1);
  ASSERT_TRUE(bufferPool.alloc(bufferHandler3) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferHandler3.m_bId == 2);
  ASSERT_TRUE(bufferPool.unpin(bufferHandler3.m_pId) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferPool.alloc(bufferHandler4) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferHandler4.m_bId == 3);
  ASSERT_TRUE(bufferPool.unpin(bufferHandler4.m_pId) == ErrorCode::E_NO_ERROR);

  ASSERT_TRUE(bufferPool.alloc(bufferHandler1) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferHandler1.m_bId == 2);
}

#if 0

/**
 * Tests reading and writing a buffer. A page is allocated to the buffer pool and then it is 
 * pinned. Some data is written into it by using a handler and finally we read it to check that
 * everything went well.
 */
TEST(BufferPoolTest, BufferPoolReadWrite) {
  FileStorage fileStorage;
  ASSERT_TRUE(fileStorage.create("./test.db", FileStorageConfig{64}, true) == ErrorCode::E_NO_ERROR);

  BufferPool bufferPool(&fileStorage, BufferPoolConfig{});

  pageId_t pId;
  ASSERT_TRUE(fileStorage.reserve(8,pId) == ErrorCode::E_NO_ERROR);

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

  BufferPool bufferPool(&fileStorage, BufferPoolConfig{});

  pageId_t pId;
  ASSERT_TRUE(fileStorage.reserve(8,pId) == ErrorCode::E_NO_ERROR);

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

#endif

SMILE_NS_END

int main(int argc, char* argv[]){
  ::testing::InitGoogleTest(&argc,argv);
  int ret = RUN_ALL_TESTS();
  return ret;
}