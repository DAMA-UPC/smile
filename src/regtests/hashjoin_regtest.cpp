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
		BufferHandler handler1, handler2;

		uint64_t page = 0;

		// HashJoin operation
		for (uint64_t i = 0; i+PAGE_SIZE_KB < DATA_KB; i += 2*PAGE_SIZE_KB) {
			if ( page%(PAGE_SIZE_KB*1024*8) == 0 ) ++page;
			ASSERT_TRUE(bufferPool.pin(page, &handler1) == ErrorCode::E_NO_ERROR);
			
			std::map<uint8_t, uint16_t> hashTable;
			uint8_t* buffer = reinterpret_cast<uint8_t*>(handler1.m_buffer);
			for (uint64_t byte = 0; byte+1 < PAGE_SIZE_KB*1024; byte += 2) {
				uint8_t key = *buffer;
				uint8_t value = *(buffer+1);
				buffer += 2;

				if (key%2 == 0) {
					std::map<uint8_t, uint16_t>::iterator it;
					it = hashTable.find(key);

					if (it == hashTable.end()) {
						hashTable.insert(std::pair<uint8_t,uint16_t>(key,value));
					}
				}
			}

			++page;
			if ( page%(PAGE_SIZE_KB*1024*8) == 0 ) ++page;
			ASSERT_TRUE(bufferPool.pin(page, &handler2) == ErrorCode::E_NO_ERROR);

			buffer = reinterpret_cast<uint8_t*>(handler2.m_buffer);
			for (uint64_t byte = 0; byte+1 < PAGE_SIZE_KB*1024; byte += 2) {
				uint8_t key = *buffer;
				uint8_t value = *(buffer+1);
				buffer += 2;

				std::map<uint8_t, uint16_t>::iterator it;
				it = hashTable.find(key);
				if (it != hashTable.end()) {
					it->second += value;
				}
			}

			ASSERT_TRUE(bufferPool.unpin(handler1.m_pId) == ErrorCode::E_NO_ERROR);
			ASSERT_TRUE(bufferPool.unpin(handler2.m_pId) == ErrorCode::E_NO_ERROR);
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