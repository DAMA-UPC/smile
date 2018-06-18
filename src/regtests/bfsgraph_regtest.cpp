#include <gtest/gtest.h>
#include <memory/buffer_pool.h>
#include <tasking/tasking.h>

SMILE_NS_BEGIN

#define PAGE_SIZE_KB 64
#define DATA_KB 4*1024*1024
#define TOTAL_NUM_BFS 100

/**
 * Performs several BFS operations over a graph stored in a DB.
 */
TEST(PerformanceTest, PerformanceTestBFSGraph) {
  	startThreadPool(1);
  	BufferPool bufferPool;
	ASSERT_TRUE(bufferPool.open(BufferPoolConfig{1024*1024}, "./graph.db") == ErrorCode::E_NO_ERROR);
	BufferHandler metaDataHandler, handler;

	// Load graph metadata
	ASSERT_TRUE(bufferPool.pin(1, &metaDataHandler) == ErrorCode::E_NO_ERROR);
	uint32_t* buffer = reinterpret_cast<uint32_t*>(metaDataHandler.m_buffer);
	uint32_t numNodes = buffer[0];
	uint32_t numEdges = buffer[1];
	uint32_t firstNbrPage = buffer[2];
	uint32_t NbrPage = buffer[3];
	uint32_t firstEdgeNode = buffer[4];
	uint32_t lastEdgeNode = buffer[5];
	uint32_t elemsPerPage = (PAGE_SIZE_KB*1024)/sizeof(uint32_t);

	for (uint32_t numBFS = 0; numBFS < TOTAL_NUM_BFS; ++numBFS) {
		std::vector<bool> visited(numNodes, false);
		std::queue<uint32_t> q;

		// Generate random starting node
		uint32_t currentNode = rand()%numNodes;
		visited[currentNode] = true;
		q.push(currentNode);

		while (!q.empty()) {
			currentNode = q.front();
			q.pop();

			// Obtain pointer to currentNode's neighbors
			uint32_t firstNbrOffset = currentNode/elemsPerPage;
			uint32_t firstNbrPosition = currentNode%elemsPerPage;
			ASSERT_TRUE(bufferPool.pin(firstNbrPage + firstNbrOffset, &handler) == ErrorCode::E_NO_ERROR);
			uint32_t* firstNbr = reinterpret_cast<uint32_t*>(handler.m_buffer);
			uint32_t currentFirstNbr = firstNbr[firstNbrPosition];

			// Discover how many neighbors the currentNode has
			uint32_t numNeighbors;
			if (currentFirstNbr == 0 && currentNode != firstEdgeNode) {
				// firstNbr points to 0. If currentNode is different than the node containing the first edge, it means that
				// currentNode has no neighbors
				numNeighbors = 0;
			}
			else if (currentNode == lastEdgeNode) {
				// Since currentNode is the last one, it will have as much neighbors as edges are left
				numNeighbors = (numEdges-1) - currentFirstNbr;
			}
			else {
				// currentNode will have as much neighbors as edges exist between its firstNbr and the firstNbr of the next
				// node with edges
				bool found = false;
				uint32_t nextNode = currentNode + 1;
				while (!found) {
					uint32_t nextOffset = nextNode/elemsPerPage;
					uint32_t nextPosition = nextNode%elemsPerPage;
					BufferHandler nextHandler;
					ASSERT_TRUE(bufferPool.pin(firstNbrPage + nextOffset, &nextHandler) == ErrorCode::E_NO_ERROR);
					uint32_t* nextfirstNbr = reinterpret_cast<uint32_t*>(nextHandler.m_buffer);
					if (nextfirstNbr[nextPosition] != 0) {
						found = true;
						numNeighbors = nextfirstNbr[nextPosition] - currentFirstNbr;
					}
					ASSERT_TRUE(bufferPool.unpin(nextHandler.m_pId) == ErrorCode::E_NO_ERROR);
					++nextNode;
				}
			}

			ASSERT_TRUE(bufferPool.unpin(handler.m_pId) == ErrorCode::E_NO_ERROR);

			// Queue not visited neighbors
			for (uint32_t i = 0; i < numNeighbors; ++i) {
				uint32_t NbrOffset = currentFirstNbr/elemsPerPage;
				uint32_t NbrPosition = currentFirstNbr%elemsPerPage;
				ASSERT_TRUE(bufferPool.pin(NbrPage + NbrOffset, &handler) == ErrorCode::E_NO_ERROR);
				uint32_t* Nbr = reinterpret_cast<uint32_t*>(handler.m_buffer);
				uint32_t neighbor = Nbr[NbrPosition];

				if (!visited[neighbor]) {
					visited[neighbor] = true;
					q.push(neighbor);
				}

				ASSERT_TRUE(bufferPool.unpin(handler.m_pId) == ErrorCode::E_NO_ERROR);
				++currentFirstNbr;
			}
		}		
	}

	stopThreadPool();
	ASSERT_TRUE(bufferPool.close() == ErrorCode::E_NO_ERROR);
}

SMILE_NS_END

int main(int argc, char* argv[]){
	::testing::InitGoogleTest(&argc,argv);
	int ret = RUN_ALL_TESTS();
	return ret;
}