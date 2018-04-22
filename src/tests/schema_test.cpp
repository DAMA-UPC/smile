
#include <gtest/gtest.h>
#include <data/schema.h>
#include <memory/buffer_pool.h>
#include <tasking/tasking.h>
#include <sstream>

SMILE_NS_BEGIN

TEST(SchemaTest, SchemaPersistence) {
  startThreadPool(1);

  BufferPool bufferPool;
  ASSERT_TRUE(bufferPool.create(BufferPoolConfig{256}, "./test.db", FileStorageConfig{4}, true) == ErrorCode::E_NO_ERROR);

  // Allocating first page, which should be 1. 
  BufferHandler handler;
  bufferPool.alloc(&handler);
  ASSERT_TRUE(handler.m_pId == 1);

  // Setting the first page to a valid state
  SchemaPageHeader* header = reinterpret_cast<SchemaPageHeader*>(handler.m_buffer);
  header->m_numElements = 0;
  header->m_nextPage = INVALID_PAGE_ID;

  bufferPool.setPageDirty(handler.m_pId);
  bufferPool.unpin(handler.m_pId);

  Schema schema{&bufferPool};
  ASSERT_TRUE(schema.loadSchema() == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(schema.persistSchema() == ErrorCode::E_NO_ERROR);

  // Creating the maximum number of node types and persisting the schema
  ASSERT_TRUE(schema.loadSchema() == ErrorCode::E_NO_ERROR);
  uint16_t maxTypes = 1 << (sizeof(typeId_t)*8 -1 );
  for(int i = 0; i < maxTypes; ++i) {
    std::stringstream ss;
    ss << "type_" << i;
    ASSERT_TRUE(schema.newNodeType(ss.str().c_str()) == ErrorCode::E_NO_ERROR);
  } 
  ASSERT_TRUE(schema.persistSchema() == ErrorCode::E_NO_ERROR);

  // Testing if the previously created node types were correctly persistend 
  ASSERT_TRUE(schema.loadSchema() == ErrorCode::E_NO_ERROR);
  for(int i = 0; i < maxTypes; ++i) {
    std::stringstream ss;
    ss << "type_" << i;
    NodeInfo nodeInfo;
    ASSERT_TRUE(schema.getNodeType(ss.str().c_str(), &nodeInfo) == ErrorCode::E_NO_ERROR);
    ASSERT_TRUE(std::string(nodeInfo.m_name) == ss.str());
    ASSERT_TRUE(nodeInfo.m_typeId == i);
  } 
  ASSERT_TRUE(schema.persistSchema() == ErrorCode::E_NO_ERROR);

  stopThreadPool();

  ASSERT_TRUE(bufferPool.close() == ErrorCode::E_NO_ERROR);
}

SMILE_NS_END

int main(int argc, char* argv[]){
  ::testing::InitGoogleTest(&argc,argv);
  int ret = RUN_ALL_TESTS();
  return ret;
}
