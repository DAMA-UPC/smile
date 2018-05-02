#include <gtest/gtest.h>
#include <memory/buffer_pool.h>
#include <tasking/tasking.h>

SMILE_NS_BEGIN

#define PAGE_SIZE_KB 64
#define DATA_KB 4*1024*1024

/**
 * Tests a Hash Join operation of 4GB over a 1GB-size Buffer Pool for benchmarking purposes.
 */
TEST(PerformanceTest, PerformanceTestHashJoin) {
	if (std::ifstream("./test.db")) {
		startThreadPool(1);

		BufferPool bufferPool;
		ASSERT_TRUE(bufferPool.open(BufferPoolConfig{1024*1024}, "./test.db") == ErrorCode::E_NO_ERROR);
		BufferHandler bufferHandler;

		uint64_t i = 0, page = 0;
		std::map<uint8_t, uint16_t> hashTable;

		// Build hash table
		for (; i < DATA_KB/16; i += PAGE_SIZE_KB) {
			if ( page%(PAGE_SIZE_KB*1024*8) == 0 ) ++page;
			ASSERT_TRUE(bufferPool.pin(page, &bufferHandler) == ErrorCode::E_NO_ERROR);
			
			uint8_t* buffer = reinterpret_cast<uint8_t*>(bufferHandler.m_buffer);
			for (uint64_t byte = 0; byte < PAGE_SIZE_KB*1024; byte += 2) {
				uint8_t key = *buffer;
				uint8_t value = *(buffer+1);
				buffer += 2;

				if (key%2 == 0) {
					std::map<uint8_t, uint16_t>::iterator it = hashTable.find(key);
					if (it == hashTable.end()) {
						hashTable.insert(std::pair<uint8_t,uint16_t>(key,value));
					}
				}
			}

			ASSERT_TRUE(bufferPool.unpin(bufferHandler.m_pId) == ErrorCode::E_NO_ERROR);
			++page;
		}

		// Probe hash table
		for (; i < DATA_KB; i += PAGE_SIZE_KB) {
			if ( page%(PAGE_SIZE_KB*1024*8) == 0 ) ++page;
			ASSERT_TRUE(bufferPool.pin(page, &bufferHandler) == ErrorCode::E_NO_ERROR);

			uint8_t* buffer = reinterpret_cast<uint8_t*>(bufferHandler.m_buffer);
			for (uint64_t byte = 0; byte < PAGE_SIZE_KB*1024; byte += 2) {
				uint8_t key = *buffer;
				uint8_t value = *(buffer+1);
				buffer += 2;

				std::map<uint8_t, uint16_t>::iterator it = hashTable.find(key);
				if (it != hashTable.end()) {
					it->second += value;
				}
			}

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