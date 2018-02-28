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

  ASSERT_TRUE(bufferPool.alloc(&bufferHandler1) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferHandler1.m_bId == 0);
  ASSERT_TRUE(bufferPool.unpin(bufferHandler1.m_pId) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferPool.alloc(&bufferHandler2) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferHandler2.m_bId == 1);
  ASSERT_TRUE(bufferPool.unpin(bufferHandler2.m_pId) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferPool.alloc(&bufferHandler3) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferHandler3.m_bId == 2);
  ASSERT_TRUE(bufferPool.unpin(bufferHandler3.m_pId) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferPool.alloc(&bufferHandler4) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferHandler4.m_bId == 3);
  ASSERT_TRUE(bufferPool.unpin(bufferHandler4.m_pId) == ErrorCode::E_NO_ERROR);

  ASSERT_TRUE(bufferPool.alloc(&bufferHandler1) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferHandler1.m_bId == 0);
  ASSERT_TRUE(bufferPool.alloc(&bufferHandler2) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferHandler2.m_bId == 1);
  ASSERT_TRUE(bufferPool.alloc(&bufferHandler3) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferHandler3.m_bId == 2);
  ASSERT_TRUE(bufferPool.unpin(bufferHandler3.m_pId) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferPool.alloc(&bufferHandler4) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferHandler4.m_bId == 3);
  ASSERT_TRUE(bufferPool.unpin(bufferHandler4.m_pId) == ErrorCode::E_NO_ERROR);

  ASSERT_TRUE(bufferPool.alloc(&bufferHandler1) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferHandler1.m_bId == 2);
}

/**
 * Tests pinning a page and writing into it. We first create a Buffer Pool with only 4-page
 * slots. Then, we allocate a page P, write into it and finally unpin it. After so, we pin
 * P again and check that it is still in its original Buffer Pool slot. We proceed to unpin
 * it and start allocating a few more pages until P is flushed to disk. At the end, we pin
 * P to bring it back to main memory and check that the data we have written still persists.
 */
TEST(BufferPoolTest, BufferPoolPinAndWritePage) {
  FileStorage fileStorage;
  ASSERT_TRUE(fileStorage.create("./test.db", FileStorageConfig{64}, true) == ErrorCode::E_NO_ERROR);
  BufferPool bufferPool(&fileStorage, BufferPoolConfig{256});
  BufferHandler bufferHandler;

  ASSERT_TRUE(bufferPool.alloc(&bufferHandler) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferHandler.m_bId == 0);
  pageId_t pId = bufferHandler.m_pId;

  bufferPool.setPageDirty(pId);
  char dataW[] = "I am writing data";
  memcpy(bufferHandler.m_buffer, dataW, 17);
  ASSERT_TRUE(bufferPool.unpin(bufferHandler.m_pId) == ErrorCode::E_NO_ERROR);

  ASSERT_TRUE(bufferPool.pin(pId, &bufferHandler) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferHandler.m_pId == pId);
  ASSERT_TRUE(bufferHandler.m_bId == 0);
  ASSERT_TRUE(bufferPool.unpin(bufferHandler.m_pId) == ErrorCode::E_NO_ERROR);

  ASSERT_TRUE(bufferPool.alloc(&bufferHandler) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferHandler.m_bId == 1);
  ASSERT_TRUE(bufferPool.unpin(bufferHandler.m_pId) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferPool.alloc(&bufferHandler) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferHandler.m_bId == 2);
  ASSERT_TRUE(bufferPool.unpin(bufferHandler.m_pId) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferPool.alloc(&bufferHandler) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferHandler.m_bId == 3);
  ASSERT_TRUE(bufferPool.unpin(bufferHandler.m_pId) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferPool.alloc(&bufferHandler) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferHandler.m_bId == 1);
  ASSERT_TRUE(bufferPool.unpin(bufferHandler.m_pId) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferPool.alloc(&bufferHandler) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferHandler.m_bId == 2);
  ASSERT_TRUE(bufferPool.unpin(bufferHandler.m_pId) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferPool.alloc(&bufferHandler) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferHandler.m_bId == 3);
  ASSERT_TRUE(bufferPool.unpin(bufferHandler.m_pId) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferPool.alloc(&bufferHandler) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferHandler.m_bId == 0);
  ASSERT_TRUE(bufferPool.unpin(bufferHandler.m_pId) == ErrorCode::E_NO_ERROR);
  
  ASSERT_TRUE(bufferPool.pin(pId, &bufferHandler) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferHandler.m_bId == 1);

  ASSERT_TRUE(bufferHandler.m_pId == pId);
  char dataR[17];
  memcpy(dataR, bufferHandler.m_buffer, 17);
  for (int i = 0; i < 17; ++i)
  {
    ASSERT_TRUE(dataW[i] == dataR[i]);
  }
}

/**
 * Tests that the Buffer Pool is properly reporting errors.
 **/
TEST(BufferPoolTest, BufferPoolErrors) {
  FileStorage fileStorage;
  ASSERT_TRUE(fileStorage.create("./test.db", FileStorageConfig{64}, true) == ErrorCode::E_NO_ERROR);
  BufferPool bufferPool(&fileStorage, BufferPoolConfig{256});
  BufferHandler bufferHandler;

  ASSERT_TRUE(bufferPool.pin(0, &bufferHandler) == ErrorCode::E_BUFPOOL_UNABLE_TO_ACCCESS_PROTECTED_PAGE);
  ASSERT_TRUE(bufferPool.unpin(0) == ErrorCode::E_BUFPOOL_UNABLE_TO_ACCCESS_PROTECTED_PAGE);
  ASSERT_TRUE(bufferPool.release(0) == ErrorCode::E_BUFPOOL_UNABLE_TO_ACCCESS_PROTECTED_PAGE);
  
  ASSERT_TRUE(bufferPool.pin(1, &bufferHandler) == ErrorCode::E_BUFPOOL_PAGE_NOT_ALLOCATED);
  ASSERT_TRUE(bufferPool.unpin(1) == ErrorCode::E_BUFPOOL_PAGE_NOT_ALLOCATED);
  ASSERT_TRUE(bufferPool.release(1) == ErrorCode::E_BUFPOOL_PAGE_NOT_ALLOCATED);

  ASSERT_TRUE(bufferPool.alloc(&bufferHandler) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferHandler.m_bId == 0);
  pageId_t pId = bufferHandler.m_pId;
  ASSERT_TRUE(bufferPool.unpin(pId) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferPool.release(pId) == ErrorCode::E_NO_ERROR);

  ASSERT_TRUE(bufferPool.unpin(1) == ErrorCode::E_BUFPOOL_PAGE_NOT_PRESENT);
  ASSERT_TRUE(bufferPool.release(1) == ErrorCode::E_NO_ERROR);

  ASSERT_TRUE(bufferPool.alloc(&bufferHandler) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferHandler.m_bId == 0);
  ASSERT_TRUE(bufferPool.alloc(&bufferHandler) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferHandler.m_bId == 1);
  ASSERT_TRUE(bufferPool.alloc(&bufferHandler) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferHandler.m_bId == 2);
  ASSERT_TRUE(bufferPool.alloc(&bufferHandler) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferHandler.m_bId == 3);

  ASSERT_TRUE(bufferPool.alloc(&bufferHandler) == ErrorCode::E_BUFPOOL_OUT_OF_MEMORY);

  ASSERT_TRUE(bufferPool.release(bufferHandler.m_pId) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferPool.alloc(&bufferHandler) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferHandler.m_bId == 3);
}

SMILE_NS_END

int main(int argc, char* argv[]){
  ::testing::InitGoogleTest(&argc,argv);
  int ret = RUN_ALL_TESTS();
  return ret;
}