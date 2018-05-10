#include <gtest/gtest.h>
#include <memory/buffer_pool.h>
#include <tasking/tasking.h>

SMILE_NS_BEGIN

#define PAGE_SIZE_KB 64
#define DATA_KB 4*1024*1024

/**
 * Tests a scan operation of 4GB over a 1GB-size Buffer Pool for benchmarking purposes.
 */
TEST(PerformanceTest, PerformanceTestScan) {
	if (std::ifstream("./test.db")) {
		startThreadPool(1);

		BufferPool bufferPool;
		ASSERT_TRUE(bufferPool.open(BufferPoolConfig{1024*1024}, "./test.db") == ErrorCode::E_NO_ERROR);
		BufferHandler bufferHandler;

		uint64_t page = 0;
		std::vector<uint64_t> dummy(PAGE_SIZE_KB*1024*8/64);

		// Scan operation
		for (uint64_t i = 0; i < DATA_KB; i += PAGE_SIZE_KB) {
			if ( page%(PAGE_SIZE_KB*1024*8) == 0 ) ++page;
			ASSERT_TRUE(bufferPool.pin(page, &bufferHandler) == ErrorCode::E_NO_ERROR);
			memcpy(&dummy[0], bufferHandler.m_buffer, PAGE_SIZE_KB*1024);
			ASSERT_TRUE(bufferPool.unpin(bufferHandler.m_pId) == ErrorCode::E_NO_ERROR);
			++page;
		}

		stopThreadPool();
	}
}

SMILE_NS_END

int main(int argc, char* argv[]){
	::testing::InitGoogleTest(&argc,argv);
	int ret = RUN_ALL_TESTS();
	return ret;
}