

#include <gtest/gtest.h>
#include <storage/file_storage.h>
#include <algorithm>

SMILE_NS_BEGIN




/** Tests the opening and closing of the file storage and checks
 * that the config information has been persisted properly
 */
TEST(FileStorageTest, FileStorageOpen) {
  FileStorage fileStorage;
  ASSERT_TRUE(fileStorage.create("./test.db", FileStorageConfig{4}, true) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(fileStorage.close() == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(fileStorage.open("./test.db") == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(fileStorage.config().m_pageSizeKB == 4);
  ASSERT_TRUE(fileStorage.close() == ErrorCode::E_NO_ERROR);
}

/**
 * Tests that pages are properly reserved and the returned pageIds 
 * are consistent with the amount of pages reserved
 */
TEST(FileStorageTest, FileStorageReserve) {
  FileStorage fileStorage;
  ASSERT_TRUE(fileStorage.create("./test.db", FileStorageConfig{64}, true) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(fileStorage.close() == ErrorCode::E_NO_ERROR);

  ASSERT_TRUE(fileStorage.open("./test.db") == ErrorCode::E_NO_ERROR);
  pageId_t pId;
  ASSERT_TRUE(fileStorage.reserve(1,&pId) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(pId == 0);
  ASSERT_TRUE(fileStorage.reserve(1,&pId) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(pId == 1);
  ASSERT_TRUE(fileStorage.reserve(4,&pId) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(pId == 2);
  ASSERT_TRUE(fileStorage.reserve(1,&pId) == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(pId == 6);
  ASSERT_TRUE(fileStorage.size() == 7);
  ASSERT_TRUE(fileStorage.close() == ErrorCode::E_NO_ERROR);
}

/**
 * Tests Read and Write operations. We first write 63 pages entirely filled with
 * one character from the contents vector (selected in a round robin fashion). 
 * Then the file storage is closed an opened again, and the contents of the
 * pages are asserted
 **/
TEST(FileStorageTest, FileStorageReadWrite) {
  FileStorage fileStorage;
  ASSERT_TRUE(fileStorage.create("./test.db", FileStorageConfig{64}, true) == ErrorCode::E_NO_ERROR);

  auto storageConfig = fileStorage.config();
  std::vector<char> data(storageConfig.m_pageSizeKB*1024);
  pageId_t pid;
  ASSERT_TRUE(fileStorage.reserve(63,&pid) == ErrorCode::E_NO_ERROR);
  std::vector<char> contents{'0','1','2','3','4','5','6','7','8','9'};
  for( auto i = pid; i < (pid+63); ++i ) {
    std::fill(data.begin(), data.end(), contents[i%contents.size()]);
    ASSERT_TRUE(fileStorage.write(data.data(),i) == ErrorCode::E_NO_ERROR);
  }
  ASSERT_TRUE(fileStorage.close() == ErrorCode::E_NO_ERROR);

  ASSERT_TRUE(fileStorage.open("./test.db") == ErrorCode::E_NO_ERROR);
  for( auto i = pid; i < (pid+63); ++i ) {
    ASSERT_TRUE(fileStorage.read(data.data(),i) == ErrorCode::E_NO_ERROR);
    for( auto it = data.begin(); it != data.end(); ++it ) {
      ASSERT_TRUE(*it == contents[i%contents.size()]);
    }
  }
  ASSERT_TRUE(fileStorage.close() == ErrorCode::E_NO_ERROR);
}

/**
 * Tests that the file storage is properly reporting errors, specially
 * for out of bounds accesses and database overwrites.
 **/
TEST(FileStorageTest, FileStorageErrors) {
  FileStorage fileStorage;
  ASSERT_TRUE(fileStorage.create("./test.db", FileStorageConfig{64}, true) == ErrorCode::E_NO_ERROR);
  auto storageConfig = fileStorage.config();
  std::vector<char> data(storageConfig.m_pageSizeKB*1024);
  ASSERT_TRUE(fileStorage.write(data.data(),63) == ErrorCode::E_STORAGE_OUT_OF_BOUNDS_PAGE);
  ASSERT_TRUE(fileStorage.read(data.data(),32) == ErrorCode::E_STORAGE_OUT_OF_BOUNDS_PAGE);
  ASSERT_TRUE(fileStorage.close() == ErrorCode::E_NO_ERROR);
  ASSERT_TRUE(fileStorage.create("./test.db", FileStorageConfig{64}) == ErrorCode::E_STORAGE_PATH_ALREADY_EXISTS);
}


SMILE_NS_END

int main(int argc, char* argv[]){
  ::testing::InitGoogleTest(&argc,argv);
  int ret = RUN_ALL_TESTS();
  return ret;
}

