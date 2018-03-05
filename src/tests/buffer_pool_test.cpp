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
  BufferPool bufferPool(&fileStorage, BufferPoolConfig{256});
  ASSERT_TRUE(bufferPool.create("./test.db", FileStorageConfig{64}, true) == ErrorCode::E_NO_ERROR);
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
  BufferPool bufferPool(&fileStorage, BufferPoolConfig{256});
  ASSERT_TRUE(bufferPool.create("./test.db", FileStorageConfig{64}, true) == ErrorCode::E_NO_ERROR);
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
  BufferPool bufferPool(&fileStorage, BufferPoolConfig{256});
  ASSERT_TRUE(bufferPool.create("./test.db", FileStorageConfig{64}, true) == ErrorCode::E_NO_ERROR);
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

/**
 * Tests that the Buffer Pool is correctly storing/retrieving the m_allocationTable 
 * information from disk.
 **/
TEST(BufferPoolTest, BufferPoolPersistence) {
  FileStorage fileStorage;
  BufferPool bufferPool(&fileStorage, BufferPoolConfig{64*8192});
  ASSERT_TRUE(bufferPool.create("./test.db", FileStorageConfig{4}, true) == ErrorCode::E_NO_ERROR);
  BufferHandler bufferHandler;

  // Allocate pages so that we need a page and a half to store the m_allocationTable.
  uint64_t pagesToAlloc = (4*1024*8)+(4*1024*4);
  for (uint64_t i = 0; i < pagesToAlloc; ++i) {
    ASSERT_TRUE(bufferPool.alloc(&bufferHandler) == ErrorCode::E_NO_ERROR);
    ASSERT_TRUE(bufferPool.unpin(bufferHandler.m_pId) == ErrorCode::E_NO_ERROR);
  }

  // Force BufferPool to flush m_allocationTable to disk.
  ASSERT_TRUE(bufferPool.checkpoint() == ErrorCode::E_NO_ERROR);

  // Read the stored m_allocationTable.
  boost::dynamic_bitset<> stored1;
  uint64_t bitsPerPage = 8*fileStorage.getPageSize(); 
  uint64_t blockSize = boost::dynamic_bitset<>::bits_per_block;
  uint64_t numTotalPages = fileStorage.size();
  uint64_t numBitmapPages = numTotalPages/bitsPerPage + (numTotalPages%bitsPerPage != 0);
  uint64_t bitmapSize = numBitmapPages*bitsPerPage;
  std::vector<boost::dynamic_bitset<>::block_type> v1(bitmapSize/blockSize);
  for (uint64_t i = 0; i < bitmapSize; i+=bitsPerPage) {
    ASSERT_TRUE(fileStorage.read(reinterpret_cast<char*>(&v1[i/blockSize]), i) == ErrorCode::E_NO_ERROR);
  }
  stored1.resize(bitmapSize);
  from_block_range(v1.begin(), v1.end(), stored1);

  // Close FileStorage
  ASSERT_TRUE(bufferPool.close() == ErrorCode::E_NO_ERROR);

  // Create a new BufferPool that uses the already existing storage.
  // It will load the m_allocationTable information from disk to memory.
  BufferPool bufferPoolAux(&fileStorage, BufferPoolConfig{64*8192});
  ASSERT_TRUE(bufferPoolAux.open("./test.db") == ErrorCode::E_NO_ERROR);

  // Force the new BufferPool to flush the loaded m_allocationTable back to disk.
  ASSERT_TRUE(bufferPoolAux.checkpoint() == ErrorCode::E_NO_ERROR);

  // Read the stored m_allocationTable by the new BufferPool.
  boost::dynamic_bitset<> stored2;
  std::vector<boost::dynamic_bitset<>::block_type> v2(bitmapSize/blockSize);
  for (uint64_t i = 0; i < numTotalPages; i+=bitsPerPage) {
    ASSERT_TRUE(fileStorage.read(reinterpret_cast<char*>(&v2[i/blockSize]), i) == ErrorCode::E_NO_ERROR);
  }
  stored2.resize(bitmapSize);
  from_block_range(v2.begin(), v2.end(), stored2);

  // Check that the both stored m_allocationTable are equal. This means, the
  // new BufferPool has read and written correctly the m_allocationTable values.
  ASSERT_TRUE(stored1.size() == stored2.size());
  for (uint64_t i = 0; i < stored1.size(); ++i) {
    ASSERT_TRUE(stored1.test(i) == stored2.test(i));
  }

  // Allocate a page with the new Buffer Pool and check that the obtained pageId_t is correct.
  // The Buffer Pool will be using 2 extra pages in order to store the m_allocationTable information.
  // Thus, the retrieved pageId_t should be pagesToAlloc+2.
  ASSERT_TRUE(bufferPoolAux.alloc(&bufferHandler) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferHandler.m_pId == pagesToAlloc+2);
}

SMILE_NS_END

int main(int argc, char* argv[]){
  ::testing::InitGoogleTest(&argc,argv);
  int ret = RUN_ALL_TESTS();
  return ret;
}