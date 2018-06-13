#include <gtest/gtest.h>
#include <memory/buffer_pool.h>
#include <tasking/tasking.h>

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

/**
 * Checks that a graph has correctly been persisted into a DB. The program loads the graph from a DB
 * and tests its correctness by comparing it to the one defined in PATH_TO_GRAPH_FILE.
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
 * Graph should be stored in the DB with the following structure:
 * 
 * 		Page:		Data:
 * 		0			Metadata --> number of nodes, number of edges, firstNbr starting page (fnp), Nbr starting page (np)
 * 		fnp - np	firstNbr structure
 * 		np - ...	Nbr structure
 * 		
 */
TEST(PerformanceTest, PerformanceTestCheckGraph) {
	if (PATH_TO_GRAPH_FILE != "" || !std::ifstream("./graph.db")) {
		uint32_t* fileFirstNbr;
		uint32_t* fileNbr;
		uint32_t fileNumNodes = 0, fileNumEdges = 0;

		// Load graph from file
		std::ifstream graphFile;
	  	graphFile.open(PATH_TO_GRAPH_FILE);
	  	if (graphFile.is_open()) {
		  	uint32_t  orig, dest, lastOrig;
		  	graphFile >> fileNumNodes;
		  	graphFile >> fileNumEdges;
		  	fileFirstNbr = new uint32_t[fileNumNodes]();
		  	fileNbr = new uint32_t[fileNumEdges]();

			for (uint32_t i = 0; i < fileNumEdges; ++i) {
				graphFile >> orig;
	  			graphFile >> dest;

	  			if (orig != lastOrig || i == 0) {
	  				fileFirstNbr[orig] = i;
	  			}
	  			lastOrig = orig;

	  			fileNbr[i] = dest;
			}
		}
	  	graphFile.close();

	  	// Open DB containing the graph
	  	startThreadPool(1);
	  	BufferPool bufferPool;
		ASSERT_TRUE(bufferPool.open(BufferPoolConfig{1024*1024}, "./graph.db") == ErrorCode::E_NO_ERROR);
		BufferHandler bufferHandler;

		// Check DB-graph metadata
		ASSERT_TRUE(bufferPool.pin(1, &bufferHandler) == ErrorCode::E_NO_ERROR);
		uint32_t* buffer = reinterpret_cast<uint32_t*>(bufferHandler.m_buffer);
		ASSERT_TRUE(buffer[0] == fileNumNodes);
		ASSERT_TRUE(buffer[1] == fileNumEdges);
		uint32_t firstNbrPage = buffer[2];
		uint32_t NbrPage = buffer[3];

		// Load DB-graph data
		uint32_t* dbFirstNbr = new uint32_t[fileNumNodes]();
		uint32_t* dbNbr = new uint32_t[fileNumEdges]();

		uint32_t elemsPerPage = (PAGE_SIZE_KB*1024)/sizeof(uint32_t);
		int remainingBytes = fileNumNodes*sizeof(uint32_t);
		for (uint32_t i = 0; i < fileNumNodes; i += elemsPerPage) {
			ASSERT_TRUE(bufferPool.pin(firstNbrPage+(i/elemsPerPage), &bufferHandler) == ErrorCode::E_NO_ERROR);
			memcpy(&dbFirstNbr[i], bufferHandler.m_buffer, std::min(PAGE_SIZE_KB*1024, remainingBytes));
			remainingBytes -= PAGE_SIZE_KB*1024;
			ASSERT_TRUE(bufferPool.unpin(bufferHandler.m_pId) == ErrorCode::E_NO_ERROR);
		}

		remainingBytes = fileNumEdges*sizeof(uint32_t);
		for (uint32_t i = 0; i < fileNumEdges; i += elemsPerPage) {
			ASSERT_TRUE(bufferPool.pin(NbrPage+(i/elemsPerPage), &bufferHandler) == ErrorCode::E_NO_ERROR);
			memcpy(&dbNbr[i], bufferHandler.m_buffer, std::min(PAGE_SIZE_KB*1024, remainingBytes));
			remainingBytes -= PAGE_SIZE_KB*1024;
			ASSERT_TRUE(bufferPool.unpin(bufferHandler.m_pId) == ErrorCode::E_NO_ERROR);
		}

		// Check firstNbr and Nbr
		for (uint32_t i = 0; i < fileNumNodes; ++i) {
			ASSERT_TRUE(fileFirstNbr[i] == dbFirstNbr[i]);
		}

		for (uint32_t i = 0; i < fileNumEdges; ++i) {
			ASSERT_TRUE(fileNbr[i] == dbNbr[i]);
		}

		stopThreadPool();
		ASSERT_TRUE(bufferPool.close() == ErrorCode::E_NO_ERROR);

		delete fileFirstNbr;
		delete fileNbr;
		delete dbFirstNbr;
		delete dbNbr;
	}
}

SMILE_NS_END

int main(int argc, char* argv[]){
	::testing::InitGoogleTest(&argc,argv);
	int ret = RUN_ALL_TESTS();
	return ret;
}