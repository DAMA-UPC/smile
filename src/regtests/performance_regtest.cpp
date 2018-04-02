#include <gtest/gtest.h>
#include <memory/buffer_pool.h>
#include "System.h"

SMILE_NS_BEGIN

/**
 * Tests a scan operation of 4GB over a 1GB-size Buffer Pool for benchmarking purposes.
 */
TEST(PerformanceTest, PerformanceTestScan) {
	uint32_t PAGE_SIZE = 64;
	BufferPool bufferPool;
	ASSERT_TRUE(bufferPool.create(BufferPoolConfig{1024*1024}, "./test.db", FileStorageConfig{PAGE_SIZE}, true) == ErrorCode::E_NO_ERROR);
	BufferHandler bufferHandler;

	std::vector<pageId_t> pages;

	// Allocate 4GB in disk before proceed scanning.
	System::profile("alloc", [&]() {
		for (uint64_t i = 0; i < 4*1024*1024; i += PAGE_SIZE) {
			ASSERT_TRUE(bufferPool.alloc(&bufferHandler) == ErrorCode::E_NO_ERROR);
			ASSERT_TRUE(bufferPool.unpin(bufferHandler.m_pId) == ErrorCode::E_NO_ERROR);
			pages.push_back(bufferHandler.m_pId);
		}
	});

	// Scan operation
	System::profile("scan", [&]() {
		for (uint64_t i = 0; i < 4*1024*1024; i += PAGE_SIZE) {
			ASSERT_TRUE(bufferPool.pin(pages[i/PAGE_SIZE], &bufferHandler) == ErrorCode::E_NO_ERROR);
			ASSERT_TRUE(bufferPool.setPageDirty(bufferHandler.m_pId) == ErrorCode::E_NO_ERROR);
			uint64_t dataToWrite = i;
			memcpy(bufferHandler.m_buffer, &dataToWrite, sizeof(uint64_t));
			ASSERT_TRUE(bufferPool.unpin(bufferHandler.m_pId) == ErrorCode::E_NO_ERROR);
		}
	});
}

SMILE_NS_END

int main(int argc, char* argv[]){
	::testing::InitGoogleTest(&argc,argv);
	int ret = RUN_ALL_TESTS();
	return ret;
}