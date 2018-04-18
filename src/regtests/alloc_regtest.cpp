#include <gtest/gtest.h>
#include <memory/buffer_pool.h>
#include "System.h"

SMILE_NS_BEGIN

#define PAGE_SIZE_KB 64
#define DATA_KB 4*1024*1024

/**
 * Tests an alloc operation of 4GB over a 1GB-size Buffer Pool for benchmarking purposes.
 */
TEST(PerformanceTest, PerformanceTestScan) {
	BufferPool bufferPool;
	ASSERT_TRUE(bufferPool.create(BufferPoolConfig{1024*1024}, "./test.db", FileStorageConfig{PAGE_SIZE_KB}, true) == ErrorCode::E_NO_ERROR);
	BufferHandler bufferHandler;

	// Allocate 4GB in disk before proceed scanning.
	System::profile("alloc", [&]() {
		for (uint64_t i = 0; i < DATA_KB; i += PAGE_SIZE_KB) {
			ASSERT_TRUE(bufferPool.alloc(&bufferHandler) == ErrorCode::E_NO_ERROR);
			ASSERT_TRUE(bufferPool.unpin(bufferHandler.m_pId) == ErrorCode::E_NO_ERROR);
		}
	});

	ASSERT_TRUE(bufferPool.close() == ErrorCode::E_NO_ERROR);
}

SMILE_NS_END

int main(int argc, char* argv[]){
	::testing::InitGoogleTest(&argc,argv);
	int ret = RUN_ALL_TESTS();
	return ret;
}