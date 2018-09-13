#include <gtest/gtest.h>
#include <memory/buffer_pool.h>
#include <tasking/tasking.h>
#include <omp.h>
#include <numa.h>

SMILE_NS_BEGIN

#define PAGE_SIZE_KB 64
#define DATA_KB 1*1024*1024
#define NUM_THREADS 4

/**
 * Tests a Group By operation of 4GB over a 1GB-size Buffer Pool for benchmarking purposes.
 */
TEST(PerformanceTest, PerformanceTestGroupByArray) {
	if (std::ifstream("./test.db")) {
		startThreadPool(0);

		BufferPool bufferPool;
		BufferPoolConfig bpConfig;
		bpConfig.m_poolSizeKB = 1024*1024;
		bpConfig.m_prefetchingDegree = 0;
		bpConfig.m_numberOfPartitions = 16;
		ASSERT_TRUE(bufferPool.open(bpConfig, "./test.db") == ErrorCode::E_NO_ERROR);

		const uint64_t numaNodes = numa_max_node() + 1;
		ASSERT_TRUE(NUM_THREADS >= numaNodes && NUM_THREADS % numaNodes == 0);

		std::array<std::array<uint8_t,256>,NUM_THREADS> occurrencesMap;
		for(uint32_t i = 0; i < NUM_THREADS; ++i) {
			for(uint32_t j = 0; j < 256; ++j) {
				occurrencesMap[i][j] = 0;
			}
		}

		// GroupBy operation
		#pragma omp parallel num_threads(NUM_THREADS)
		{
			uint64_t threadID = omp_get_thread_num();
			uint64_t numThreads = omp_get_num_threads();
			uint64_t KBRangePerThread = DATA_KB/(numThreads/numaNodes);
			uint64_t startingKB = KBRangePerThread*(threadID/numaNodes) + PAGE_SIZE_KB*(threadID%numaNodes);
			BufferHandler bufferHandler;

			for (uint64_t i = startingKB; i < KBRangePerThread + startingKB; i += PAGE_SIZE_KB * numaNodes) {
				uint64_t page = 1 + (i/PAGE_SIZE_KB)/(PAGE_SIZE_KB*1024) + (i/PAGE_SIZE_KB);
				bufferPool.pin(page, &bufferHandler);
				
				uint8_t* buffer = reinterpret_cast<uint8_t*>(bufferHandler.m_buffer);
				for (uint64_t byte = 0; byte < PAGE_SIZE_KB*1024; ++byte) {
					uint8_t number = *buffer;
					occurrencesMap[threadID][number]++;
					++buffer;
				}

				bufferPool.unpin(bufferHandler.m_pId);
			}
		}	

		#pragma omp barrier

		// Final aggregate
		for (uint64_t i = 1; i < NUM_THREADS; ++i) {
			for(uint32_t j = 0; j < 256; ++j) {
					occurrencesMap[0][j] += occurrencesMap[i][j];
			}
		}

		stopThreadPool();
    	bufferPool.close();
	}
}

SMILE_NS_END

int main(int argc, char* argv[]){
	::testing::InitGoogleTest(&argc,argv);
	int ret = RUN_ALL_TESTS();
	return ret;
}
