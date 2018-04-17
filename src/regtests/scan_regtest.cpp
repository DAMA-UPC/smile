#include <gtest/gtest.h>
#include <memory/buffer_pool.h>
#include <tasking/tasking.h>
#include "System.h"

SMILE_NS_BEGIN

#define PAGE_SIZE_KB 64
#define DATA_KB 4*1024*1024

/**
 * Tests a scan operation of 4GB over a 1GB-size Buffer Pool for benchmarking purposes.
 */
TEST(PerformanceTest, PerformanceTestScan) {
	if (std::ifstream("./test.db")) {
		startThreadPool(4);

		BufferPool bufferPool;
		ASSERT_TRUE(bufferPool.open(BufferPoolConfig{1024*1024}, "./test.db") == ErrorCode::E_NO_ERROR);
		BufferHandler bufferHandler;

		uint64_t page = 0;

		// Scan operation
		System::profile("scan", [&]() {
			for (uint64_t i = 0; i < DATA_KB; i += PAGE_SIZE_KB) {
				if ( page%(PAGE_SIZE_KB*1024*8) == 0 ) ++page;
				ASSERT_TRUE(bufferPool.pin(page, &bufferHandler) == ErrorCode::E_NO_ERROR);
				ASSERT_TRUE(bufferPool.setPageDirty(bufferHandler.m_pId) == ErrorCode::E_NO_ERROR);
				uint64_t dataToWrite = i;
				memcpy(bufferHandler.m_buffer, &dataToWrite, sizeof(uint64_t));
				ASSERT_TRUE(bufferPool.unpin(bufferHandler.m_pId) == ErrorCode::E_NO_ERROR);
				++page;
			}
		});

		stopThreadPool();
	}
}

SMILE_NS_END

int main(int argc, char* argv[]){
	::testing::InitGoogleTest(&argc,argv);
	int ret = RUN_ALL_TESTS();
	return ret;
}