#include <gtest/gtest.h>
#include <memory/buffer_pool.h>
#include <tasking/tasking.h>
#include <thread>

SMILE_NS_BEGIN

/**
 * Tests Buffer Pool allocations. We first declare a 4-slot (256 KB) Buffer Pool (64 KB page
 * size). Then, we ask for allocating 4 pages into the pool and unpin them. With the pool full,
 * we try to allocate space for 4 other pages and check that the slots are being reused.
 * Finally, a couple of buffers are unpinned before allocating another page in order to see
 * that the Clock Sweep Algorithm is behaving as expected.
 */
TEST(BufferPoolTest, BufferPoolAlloc) {
  startThreadPool(1);
  BufferPool bufferPool;
  BufferPoolConfig bpConfig;
  bpConfig.m_poolSizeKB = 256;
  bpConfig.m_prefetchingDegree = 0;
  bpConfig.m_numberOfPartitions = 1;
  ASSERT_TRUE(bufferPool.create(bpConfig, "./test.db", FileStorageConfig{64}, true) == ErrorCode::E_NO_ERROR);
  BufferHandler bufferHandler1, bufferHandler2, bufferHandler3, bufferHandler4;

  ASSERT_TRUE(bufferPool.alloc(&bufferHandler1) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferHandler1.m_bId == 0);
  ASSERT_TRUE(bufferPool.unpin(bufferHandler1) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferPool.alloc(&bufferHandler2) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferHandler2.m_bId == 1);
  ASSERT_TRUE(bufferPool.unpin(bufferHandler2) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferPool.alloc(&bufferHandler3) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferHandler3.m_bId == 2);
  ASSERT_TRUE(bufferPool.unpin(bufferHandler3) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferPool.alloc(&bufferHandler4) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferHandler4.m_bId == 3);
  ASSERT_TRUE(bufferPool.unpin(bufferHandler4) == ErrorCode::E_NO_ERROR);

  ASSERT_TRUE(bufferPool.alloc(&bufferHandler1) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferHandler1.m_bId == 0);
  ASSERT_TRUE(bufferPool.alloc(&bufferHandler2) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferHandler2.m_bId == 1);
  ASSERT_TRUE(bufferPool.alloc(&bufferHandler3) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferHandler3.m_bId == 2);
  ASSERT_TRUE(bufferPool.unpin(bufferHandler3) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferPool.alloc(&bufferHandler4) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferHandler4.m_bId == 3);
  ASSERT_TRUE(bufferPool.unpin(bufferHandler4) == ErrorCode::E_NO_ERROR);

  ASSERT_TRUE(bufferPool.alloc(&bufferHandler1) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferHandler1.m_bId == 2);
  stopThreadPool();
  ASSERT_TRUE(bufferPool.close() == ErrorCode::E_NO_ERROR);
}

/**
 * Tests pinning a page and writing into it. We first create a Buffer Pool with only 4-page
 * slots. Then, we allocate a page P, write into it and finally unpin it. After so, we pin
 * P again and check that it is still in its original Buffer Pool slot. We proceed to unpin
 * it and start allocating a few more pages until P is flushed to disk. At the end, we pin
 * P to bring it back to main memory and check that the data we have written still persists.
 */
TEST(BufferPoolTest, BufferPoolPinAndWritePage) {
  startThreadPool(1);

  BufferPool bufferPool;
  BufferPoolConfig bpConfig;
  bpConfig.m_poolSizeKB = 256;
  bpConfig.m_prefetchingDegree = 0;
  bpConfig.m_numberOfPartitions = 1;
  ASSERT_TRUE(bufferPool.create(bpConfig, "./test.db", FileStorageConfig{64}, true) == ErrorCode::E_NO_ERROR);
  BufferHandler bufferHandler;

  ASSERT_TRUE(bufferPool.alloc(&bufferHandler) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferHandler.m_bId == 0);
  pageId_t pId = bufferHandler.m_pId;

  bufferPool.setPageDirty(pId);
  char dataW[] = "I am writing data";
  memcpy(bufferHandler.m_buffer, dataW, 17);
  ASSERT_TRUE(bufferPool.unpin(bufferHandler) == ErrorCode::E_NO_ERROR);

  ASSERT_TRUE(bufferPool.pin(pId, &bufferHandler) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferHandler.m_pId == pId);
  ASSERT_TRUE(bufferHandler.m_bId == 0);
  ASSERT_TRUE(bufferPool.unpin(bufferHandler) == ErrorCode::E_NO_ERROR);

  ASSERT_TRUE(bufferPool.alloc(&bufferHandler) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferHandler.m_bId == 1);
  ASSERT_TRUE(bufferPool.unpin(bufferHandler) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferPool.alloc(&bufferHandler) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferHandler.m_bId == 2);
  ASSERT_TRUE(bufferPool.unpin(bufferHandler) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferPool.alloc(&bufferHandler) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferHandler.m_bId == 3);
  ASSERT_TRUE(bufferPool.unpin(bufferHandler) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferPool.alloc(&bufferHandler) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferHandler.m_bId == 1);
  ASSERT_TRUE(bufferPool.unpin(bufferHandler) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferPool.alloc(&bufferHandler) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferHandler.m_bId == 2);
  ASSERT_TRUE(bufferPool.unpin(bufferHandler) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferPool.alloc(&bufferHandler) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferHandler.m_bId == 3);
  ASSERT_TRUE(bufferPool.unpin(bufferHandler) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferPool.alloc(&bufferHandler) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferHandler.m_bId == 0);
  ASSERT_TRUE(bufferPool.unpin(bufferHandler) == ErrorCode::E_NO_ERROR);
  
  ASSERT_TRUE(bufferPool.pin(pId, &bufferHandler) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferHandler.m_bId == 1);

  ASSERT_TRUE(bufferHandler.m_pId == pId);
  char dataR[17];
  memcpy(dataR, bufferHandler.m_buffer, 17);
  for (int i = 0; i < 17; ++i)
  {
    ASSERT_TRUE(dataW[i] == dataR[i]);
  }

  stopThreadPool();
  ASSERT_TRUE(bufferPool.close() == ErrorCode::E_NO_ERROR);
}

///**
// * Tests that the Buffer Pool is properly reporting errors.
// **/
TEST(BufferPoolTest, BufferPoolErrors) {
  startThreadPool(1);
  BufferPool bufferPool;
  BufferPoolConfig bpConfig;
  bpConfig.m_poolSizeKB = 256;
  bpConfig.m_prefetchingDegree = 0;
  bpConfig.m_numberOfPartitions = 1;
  ASSERT_TRUE(bufferPool.create(bpConfig, "./test.db", FileStorageConfig{64}, true) == ErrorCode::E_NO_ERROR);
  BufferHandler bufferHandler;

  ASSERT_TRUE(bufferPool.alloc(&bufferHandler) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferHandler.m_bId == 0);
  ASSERT_TRUE(bufferPool.unpin(bufferHandler) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferPool.release(bufferHandler.m_pId) == ErrorCode::E_NO_ERROR);

  ASSERT_TRUE(bufferPool.release(1) == ErrorCode::E_NO_ERROR);

  ASSERT_TRUE(bufferPool.alloc(&bufferHandler) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferHandler.m_bId == 1);
  ASSERT_TRUE(bufferPool.alloc(&bufferHandler) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferHandler.m_bId == 2);
  ASSERT_TRUE(bufferPool.alloc(&bufferHandler) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferHandler.m_bId == 3);
  ASSERT_TRUE(bufferPool.alloc(&bufferHandler) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferHandler.m_bId == 0);

  ASSERT_TRUE(bufferPool.alloc(&bufferHandler) == ErrorCode::E_BUFPOOL_OUT_OF_MEMORY);

  ASSERT_TRUE(bufferPool.release(bufferHandler.m_pId) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferPool.alloc(&bufferHandler) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferHandler.m_bId == 0);

  stopThreadPool();
  ASSERT_TRUE(bufferPool.close() == ErrorCode::E_NO_ERROR);
  startThreadPool(1);
  ASSERT_TRUE(bufferPool.open(BufferPoolConfig{257}, "./test.db") == ErrorCode::E_BUFPOOL_POOL_SIZE_NOT_MULTIPLE_OF_PAGE_SIZE);
  ASSERT_TRUE(bufferPool.create(BufferPoolConfig{257}, "./test.db", FileStorageConfig{64}, true) == ErrorCode::E_BUFPOOL_POOL_SIZE_NOT_MULTIPLE_OF_PAGE_SIZE);

  stopThreadPool();
}

/**
 * Tests that the Buffer Pool is correctly storing/retrieving the m_allocationTable 
 * information from disk.
 **/
TEST(BufferPoolTest, BufferPoolPersistence) {
  startThreadPool(1);
  BufferPool bufferPool;
  BufferPoolConfig bpConfig;
  bpConfig.m_poolSizeKB = 64*8192;
  bpConfig.m_prefetchingDegree = 0;
  bpConfig.m_numberOfPartitions = 1;
  ASSERT_TRUE(bufferPool.create(bpConfig, "./test.db", FileStorageConfig{4}, true) == ErrorCode::E_NO_ERROR);
  BufferHandler bufferHandler;

  // Allocate pages so that we need a page and a half to store the m_allocationTable.
  uint64_t pagesToAlloc = (4*1024*8)+(4*1024*4);
  for (uint64_t i = 0; i < pagesToAlloc; ++i) {
    ASSERT_TRUE(bufferPool.alloc(&bufferHandler) == ErrorCode::E_NO_ERROR);
    ASSERT_TRUE(bufferPool.unpin(bufferHandler) == ErrorCode::E_NO_ERROR);
  }

  // Force BufferPool to flush m_allocationTable to disk.
  ASSERT_TRUE(bufferPool.close() == ErrorCode::E_NO_ERROR);

  // Create a new BufferPool that uses the already existing storage.
  // It will load the m_allocationTable information from disk to memory.
  BufferPool bufferPoolAux;
  ASSERT_TRUE(bufferPoolAux.open(bpConfig, "./test.db") == ErrorCode::E_NO_ERROR);

  // Allocate a page with the new Buffer Pool and check that the obtained pageId_t is correct.
  // The Buffer Pool will be using 2 extra pages in order to store the m_allocationTable information.
  // Thus, the retrieved pageId_t should be pagesToAlloc+2.
  ASSERT_TRUE(bufferPoolAux.alloc(&bufferHandler) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(bufferHandler.m_pId == pagesToAlloc+2);

  stopThreadPool();
  ASSERT_TRUE(bufferPoolAux.close() == ErrorCode::E_NO_ERROR);
}

/**
 * Used by BufferPoolThreadSafe.
 */
struct Params {
  Params() {}
  Params(BufferPool* bp, BufferHandler handler) {
    p_bp = bp;
    m_handler = handler;
  }

  BufferPool* p_bp;
  BufferHandler m_handler;
};

/**
 * Used by BufferPoolThreadSafe.
 */
void release(Params* params) {
  ASSERT_TRUE(params->p_bp->release(params->m_handler.m_pId) == ErrorCode::E_NO_ERROR);
}

/**
 * Used by BufferPoolThreadSafe.
 */
void unpin(Params* params) {
  ASSERT_TRUE(params->p_bp->setPageDirty(params->m_handler.m_pId) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(params->p_bp->unpin(params->m_handler) == ErrorCode::E_NO_ERROR);
}

/**
 * Used by BufferPoolThreadSafe.
 */
void checkpoint(Params* params) {
  ASSERT_TRUE(params->p_bp->checkpoint() == ErrorCode::E_NO_ERROR);
}

/**
 * Tests that the buffer pool is thread safe. In order to do so, a 1GB-buffer-pool is created and later
 * several alloc/release/unpin/checkpoint/setPageDirty operations are used by different threads. Finally,
 * we check that the state of the Buffer Pool is consistent.
 */
TEST(BufferPoolTest, BufferPoolThreadSafe) {
  // 16384 buffers * 64 KB/page = 1 GB of buffer pool.
  startThreadPool(1);
  uint32_t poolSize = 16384;
  uint32_t allocatedPages = 0;
  BufferPool bufferPool;
  BufferPoolConfig bpConfig;
  bpConfig.m_poolSizeKB = 64*poolSize;
  bpConfig.m_prefetchingDegree = 0;
  bpConfig.m_numberOfPartitions = 1;
  ASSERT_TRUE(bufferPool.create(bpConfig, "./test.db", FileStorageConfig{64}, true) == ErrorCode::E_NO_ERROR);
  BufferHandler bufferHandler;

  std::vector<std::thread> threads;
  std::vector<Params> params(poolSize);

  for (uint64_t i = 0; i < poolSize-1; ++i) {
    ASSERT_TRUE(bufferPool.alloc(&bufferHandler) == ErrorCode::E_NO_ERROR);
    ++allocatedPages;

    Params aux(&bufferPool, bufferHandler);    
    params[i] = aux; 

    bool doRelease = ((rand()%2) == 0);
    if (doRelease) {
      threads.push_back(std::thread(release, &params[i]));
      --allocatedPages;
    }
    else {
      threads.push_back(std::thread(unpin, &params[i]));
    }

    if ((i%256) == 0) {
      threads.push_back(std::thread(checkpoint, &params[i]));
    }
  }

  for (auto& th : threads) {
    th.join();
  }

  BufferPoolStatistics stats;
  ASSERT_TRUE(bufferPool.getStatistics(&stats) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(stats.m_numAllocatedPages == allocatedPages);
  
  ASSERT_TRUE(bufferPool.checkConsistency() == ErrorCode::E_NO_ERROR);

  stopThreadPool();
  ASSERT_TRUE(bufferPool.close() == ErrorCode::E_NO_ERROR);
}


SMILE_NS_END

int main(int argc, char* argv[]){
  ::testing::InitGoogleTest(&argc,argv);
  int ret = RUN_ALL_TESTS();
  return ret;
}
