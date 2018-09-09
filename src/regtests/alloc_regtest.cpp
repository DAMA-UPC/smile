#include <gtest/gtest.h>
#include <memory/buffer_pool.h>
#include "../tasking/tasking.h"

SMILE_NS_BEGIN

#define PAGE_SIZE_KB 64
#define DATA_KB 4*1024*1024

/**
 * Tests an alloc operation of 4GB over a 1GB-size Buffer Pool for benchmarking purposes.
 */
TEST(PerformanceTest, PerformanceTestScan) {
	startThreadPool(1);
	BufferPool bufferPool;
	BufferPoolConfig bpConfig;
	bpConfig.m_poolSizeKB = 1024*1024;
	bpConfig.m_prefetchingDegree = 1;
	bpConfig.m_numberOfPartitions = 16;
	ASSERT_TRUE(bufferPool.create(bpConfig, "./test.db", FileStorageConfig{PAGE_SIZE_KB}, true) == ErrorCode::E_NO_ERROR);
	BufferHandler bufferHandler;

	// Allocate 4GB in disk before proceed scanning.
	for (uint64_t i = 0; i < DATA_KB; i += PAGE_SIZE_KB) {
		ASSERT_TRUE(bufferPool.alloc(&bufferHandler) == ErrorCode::E_NO_ERROR);
		ASSERT_TRUE(bufferPool.setPageDirty(bufferHandler.m_pId) == ErrorCode::E_NO_ERROR);

		uint8_t* buffer = reinterpret_cast<uint8_t*>(bufferHandler.m_buffer);
		for (uint64_t byte = 0; byte < PAGE_SIZE_KB*1024; ++byte) {
			uint8_t random = rand()%256;
			*buffer = random;
			++buffer;
		}

		ASSERT_TRUE(bufferPool.unpin(bufferHandler.m_pId) == ErrorCode::E_NO_ERROR);
	}

 	stopThreadPool();
	ASSERT_TRUE(bufferPool.close() == ErrorCode::E_NO_ERROR);
}

SMILE_NS_END

int main(int argc, char* argv[]){
	::testing::InitGoogleTest(&argc,argv);
	int ret = RUN_ALL_TESTS();
	return ret;
}
