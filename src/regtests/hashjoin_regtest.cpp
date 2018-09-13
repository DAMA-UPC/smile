#include <gtest/gtest.h>
#include <memory/buffer_pool.h>
#include <tasking/tasking.h>
#include <omp.h>
#include <numa.h>

SMILE_NS_BEGIN

#define PAGE_SIZE_KB 64
#define DATA_KB 4*1024*1024
#define NUM_THREADS 8

/**
 * Tests a Hash Join operation of 4GB over a 1GB-size Buffer Pool for benchmarking purposes.
 */
TEST(PerformanceTest, PerformanceTestHashJoin) {
	if (std::ifstream("./test.db")) {
		startThreadPool(1);

		BufferPool bufferPool;
		BufferPoolConfig bpConfig;
		bpConfig.m_poolSizeKB = 1024*1024;
		bpConfig.m_prefetchingDegree = 1;
		ASSERT_TRUE(bufferPool.open(bpConfig, "./test.db") == ErrorCode::E_NO_ERROR);

		std::array<std::map<uint8_t,uint16_t>,NUM_THREADS> hashTable;
		const uint64_t numaNodes = numa_max_node() + 1;
		ASSERT_TRUE(NUM_THREADS >= numaNodes && NUM_THREADS % numaNodes == 0);

		// Build partial hash tables
		#pragma omp parallel num_threads(NUM_THREADS)
		{
			uint64_t threadID = omp_get_thread_num();
			uint64_t numThreads = omp_get_num_threads();
			uint64_t KBRangePerThread = DATA_KB/(numThreads/numaNodes);
			uint64_t startingKB = KBRangePerThread*(threadID/numaNodes) + PAGE_SIZE_KB*(threadID%numaNodes);
			BufferHandler bufferHandler;

			for (uint64_t i = startingKB; i < KBRangePerThread + startingKB; i += PAGE_SIZE_KB * numaNodes) {
				uint64_t page = 1 + (i/PAGE_SIZE_KB)/(PAGE_SIZE_KB*1024) + (i/PAGE_SIZE_KB);
				bufferPool.pin(page, &bufferHandler) == ErrorCode::E_NO_ERROR;
				
				uint8_t* buffer = reinterpret_cast<uint8_t*>(bufferHandler.m_buffer);
				for (uint64_t byte = 0; byte < PAGE_SIZE_KB*1024; byte += 2) {
					uint8_t key = *buffer;
					uint8_t value = *(buffer+1);
					buffer += 2;

					if (key%2 == 0) {
						std::map<uint8_t, uint16_t>::iterator it = hashTable[threadID].find(key);
						if (it == hashTable[threadID].end()) {
							hashTable[threadID].insert(std::pair<uint8_t,uint16_t>(key,value));
						}
					}
				}

				bufferPool.unpin(bufferHandler.m_pId) == ErrorCode::E_NO_ERROR;
			}
		}

		#pragma omp barrier

		// Merge partial tables into global hash table
		for (uint64_t i = 1; i < NUM_THREADS; ++i) {
			std::map<uint8_t, uint16_t>::iterator it = hashTable[i].begin();
			while ( it != hashTable[i].end()) {
				std::map<uint8_t, uint16_t>::iterator it2;
				it2 = hashTable[0].find(it->first);
				if (it2 == hashTable[0].end()) {
					hashTable[0].insert(std::pair<uint8_t,uint16_t>(it->first,it->second));
				}
				++it;
			}
		}

		// Probe global hash table
		#pragma omp parallel num_threads(NUM_THREADS)
		{
			uint64_t threadID = omp_get_thread_num();
			uint64_t numThreads = omp_get_num_threads();
			uint64_t KBPerThread = (DATA_KB-DATA_KB/16)/numThreads;
			BufferHandler bufferHandler;

			for (uint64_t i = DATA_KB/16+threadID*KBPerThread; (i-threadID*KBPerThread) < KBPerThread; i += PAGE_SIZE_KB) {
				uint64_t page = 1 + (i/PAGE_SIZE_KB)/(PAGE_SIZE_KB*1024) + (i/PAGE_SIZE_KB) + ((DATA_KB/16)/PAGE_SIZE_KB);
				bufferPool.pin(page, &bufferHandler) == ErrorCode::E_NO_ERROR;

				uint8_t* buffer = reinterpret_cast<uint8_t*>(bufferHandler.m_buffer);
				for (uint64_t byte = 0; byte < PAGE_SIZE_KB*1024; byte += 2) {
					uint8_t key = *buffer;
					uint8_t value = *(buffer+1);
					buffer += 2;

					std::map<uint8_t, uint16_t>::iterator it = hashTable[0].find(key);
					if (it != hashTable[0].end()) {
						#pragma omp atomic
						it->second += value;
					}
				}

				bufferPool.unpin(bufferHandler.m_pId) == ErrorCode::E_NO_ERROR;
			}
		}

		#pragma omp barrier

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
