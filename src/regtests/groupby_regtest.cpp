#include <gtest/gtest.h>
#include <memory/buffer_pool.h>
#include <tasking/tasking.h>
#include <omp.h>

SMILE_NS_BEGIN

#define PAGE_SIZE_KB 64
#define DATA_KB 4*1024*1024
#define NUM_THREADS 4

/**
 * Tests a Group By operation of 4GB over a 1GB-size Buffer Pool for benchmarking purposes.
 */
TEST(PerformanceTest, PerformanceTestGroupBy) {
	if (std::ifstream("./test.db")) {
		startThreadPool(1);

		BufferPool bufferPool;
		ASSERT_TRUE(bufferPool.open(BufferPoolConfig{1024*1024}, "./test.db") == ErrorCode::E_NO_ERROR);
		BufferHandler bufferHandler;

		// GroupBy operation
		#pragma omp parallel num_threads(NUM_THREADS)
		{
			uint64_t threadID = omp_get_thread_num();
			uint64_t numThreads = omp_get_num_threads();
			uint64_t KBPerThread = DATA_KB/numThreads;

			for (uint64_t i = threadID*KBPerThread; (i-threadID*KBPerThread) < KBPerThread; i += PAGE_SIZE_KB) {
				uint64_t page = 1 + (i/PAGE_SIZE_KB)/(PAGE_SIZE_KB*1024) + (i/PAGE_SIZE_KB);
				bufferPool.pin(page, &bufferHandler);
				
				std::map<uint8_t, uint16_t> occurrencesMap;
				uint8_t* buffer = reinterpret_cast<uint8_t*>(bufferHandler.m_buffer);
				for (uint64_t byte = 0; byte < PAGE_SIZE_KB*1024; ++byte) {
					uint8_t number = *buffer;
					std::map<uint8_t, uint16_t>::iterator it;
					it = occurrencesMap.find(number);

					if (it == occurrencesMap.end()) {
						occurrencesMap.insert(std::pair<uint8_t,uint16_t>(number,1));
					}
					else {
						++it->second;
					}

					++buffer;
				}

				bufferPool.unpin(bufferHandler.m_pId) == ErrorCode::E_NO_ERROR;
			}
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