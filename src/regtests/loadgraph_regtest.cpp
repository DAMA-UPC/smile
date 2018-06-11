#include <gtest/gtest.h>
#include <memory/buffer_pool.h>

SMILE_NS_BEGIN

#define PAGE_SIZE_KB 64
#define DATA_KB 4*1024*1024
#define PATH_TO_GRAPH_FILE ""

/**
 * Reads a graph from a file and stores it in a DB using a buffer pool.
 * 
 * Graph files must have the following structure:
 * 
 * 		number_of_nodes		number_of_edges
 * 		node1				neighbor1_of_node1
 * 		node1				neighbor2_of_node1
 * 		node2				neighbor1_of_node2
 * 		node3				neighbor1_of_node3
 * 		node3				neighbor2_of_node3
 * 		...
 * 
 * Graph is stored in the DB with the following structure:
 * 
 * 		Page:		Data:
 * 		0			Metadata --> number of nodes, number of edges, firstNbr starting page (fnp), Nbr starting page (np)
 * 		fnp - np	firstNbr structure
 * 		np - ...	Nbr structure
 * 		
 */
TEST(PerformanceTest, PerformanceTestLoadGraph) {
	if (PATH_TO_GRAPH_FILE != "") {
		// Graph data
		uint32_t* firstNbr;
		uint32_t* Nbr;
		uint32_t numNodes = 0, numEdges = 0;

		// Load data from file
		std::ifstream graphFile;
	  	graphFile.open(PATH_TO_GRAPH_FILE);
	  	if (graphFile.is_open()) {
		  	uint32_t  orig, dest, lastOrig;
		  	graphFile >> numNodes;
		  	graphFile >> numEdges;
		  	firstNbr = new uint32_t[numNodes]();
		  	Nbr = new uint32_t[numEdges]();

			for (uint32_t i = 0; i < numEdges; ++i) {
				graphFile >> orig;
	  			graphFile >> dest;

	  			if (orig != lastOrig || i == 0) {
	  				firstNbr[orig] = i;
	  			}
	  			lastOrig = orig;

	  			Nbr[i] = dest;
			}
		}
	  	graphFile.close();

	  	// Save graph into DB
	  	BufferPool bufferPool;
		ASSERT_TRUE(bufferPool.create(BufferPoolConfig{1024*1024}, "./graph.db", FileStorageConfig{PAGE_SIZE_KB}, true) == ErrorCode::E_NO_ERROR);
		BufferHandler metaDataHandler, dataHandler;

		// Save graph metadata
		ASSERT_TRUE(bufferPool.alloc(&metaDataHandler) == ErrorCode::E_NO_ERROR);
		ASSERT_TRUE(bufferPool.setPageDirty(metaDataHandler.m_pId) == ErrorCode::E_NO_ERROR);
		uint32_t* buffer = reinterpret_cast<uint32_t*>(metaDataHandler.m_buffer);
		buffer[0] = numNodes;
		buffer[1] = numEdges;

		// Save firstNbr
		uint32_t elemsPerPage = (PAGE_SIZE_KB*1024)/sizeof(uint32_t);
		int remainingBytes = numNodes*sizeof(uint32_t);
		for (uint32_t i = 0; i < numNodes; i += elemsPerPage) {
			ASSERT_TRUE(bufferPool.alloc(&dataHandler) == ErrorCode::E_NO_ERROR);
			ASSERT_TRUE(bufferPool.setPageDirty(dataHandler.m_pId) == ErrorCode::E_NO_ERROR);
			memcpy(dataHandler.m_buffer, &firstNbr[i], std::min(PAGE_SIZE_KB*1024, remainingBytes));
			remainingBytes -= PAGE_SIZE_KB*1024;
			ASSERT_TRUE(bufferPool.unpin(dataHandler.m_pId) == ErrorCode::E_NO_ERROR);
			if (i == 0) {
				buffer[2] = dataHandler.m_pId;
			}
		}

		// Save Nbr
		remainingBytes = numEdges*sizeof(uint32_t);
		for (uint32_t i = 0; i < numEdges; i += elemsPerPage) {
			ASSERT_TRUE(bufferPool.alloc(&dataHandler) == ErrorCode::E_NO_ERROR);
			ASSERT_TRUE(bufferPool.setPageDirty(dataHandler.m_pId) == ErrorCode::E_NO_ERROR);
			memcpy(dataHandler.m_buffer, &Nbr[i], std::min(PAGE_SIZE_KB*1024, remainingBytes));
			remainingBytes -= PAGE_SIZE_KB*1024;
			ASSERT_TRUE(bufferPool.unpin(dataHandler.m_pId) == ErrorCode::E_NO_ERROR);
			if (i == 0) {
				buffer[3] = dataHandler.m_pId;
			}
		}

		ASSERT_TRUE(bufferPool.unpin(metaDataHandler.m_pId) == ErrorCode::E_NO_ERROR);
		ASSERT_TRUE(bufferPool.close() == ErrorCode::E_NO_ERROR);

		delete firstNbr;
		delete Nbr;
	}
}

SMILE_NS_END

int main(int argc, char* argv[]){
	::testing::InitGoogleTest(&argc,argv);
	int ret = RUN_ALL_TESTS();
	return ret;
}