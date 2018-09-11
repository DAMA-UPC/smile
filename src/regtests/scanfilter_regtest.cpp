#include <gtest/gtest.h>
#include <memory/buffer_pool.h>
#include <tasking/tasking.h>
#include <omp.h>

SMILE_NS_BEGIN

#define PAGE_SIZE_KB 64
#define DATA_KB 1*1024*1024
#define NUM_THREADS 1

/**
 * Tests a scan operation which filters the values found in a set of pages for benchmarking purposes.
 */
TEST(PerformanceTest, PerformanceTestScanFilter) {
	if (std::ifstream("./test.db")) {
		startThreadPool(0);

		BufferPool bufferPool;
		BufferPoolConfig bpConfig;
		bpConfig.m_poolSizeKB = 1024*1024;
		bpConfig.m_prefetchingDegree = 0;
		bpConfig.m_numberOfPartitions = 128;
		ASSERT_TRUE(bufferPool.open(bpConfig, "./test.db") == ErrorCode::E_NO_ERROR);

		uint32_t threshold = 2^32/2;
		uint32_t counter = 0;
		std::array<uint32_t,NUM_THREADS> partialCounter;

		// Scan Filter operation
		#pragma omp parallel num_threads(NUM_THREADS)
		{
			uint64_t threadID = omp_get_thread_num();
			uint64_t numThreads = omp_get_num_threads();
			uint64_t KBPerThread = DATA_KB/numThreads;
			BufferHandler bufferHandler;

			for (uint64_t i = threadID*KBPerThread; (i-threadID*KBPerThread) < KBPerThread; i += PAGE_SIZE_KB) {
				uint64_t page = 1 + (i/PAGE_SIZE_KB)/(PAGE_SIZE_KB*1024) + (i/PAGE_SIZE_KB);
				bufferPool.pin(page, &bufferHandler);
				
				uint32_t* buffer = reinterpret_cast<uint32_t*>(bufferHandler.m_buffer);
				for (uint64_t byte = 0; byte < PAGE_SIZE_KB*1024; byte += 4) {
					uint32_t number = *buffer;
					if (number > threshold) {
						++partialCounter[threadID];
					}
					++buffer;
				}

				bufferPool.unpin(bufferHandler.m_pId);
			}
		}	

		#pragma omp barrier

		// Final aggregate
		for (uint64_t i = 1; i < NUM_THREADS; ++i) {
			counter += partialCounter[i];
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
