
#ifndef _STORAGE_FILE_STORAGE_H_
#define _STORAGE_FILE_STORAGE_H_ 

#include "../base/base.h"
#include "types.h"
#include <vector>

#include <fstream>

SMILE_NS_BEGIN

struct FileStorageConfig {
  uint32_t  m_pageSizeKB = 64;
};

class FileStorage {
  public:
    SMILE_NOT_COPYABLE(FileStorage)

    FileStorage() noexcept;

    virtual ~FileStorage() noexcept;

    /**
     * Opens the file storage at the given path
     * @param in the path to the storage
     * @return true if the storage was opened correctly
     **/
    ErrorCode open( const std::string& path );

    /**
     * Opens the file storage at the given path
     * @param in the path to the storage to create
     * @return in the configuration of the storage
     **/
    ErrorCode create( const std::string& path, const FileStorageConfig& config, const bool& overwrite = false );

    /**
     * Closes the storage
     * @return true if the closing was successful
     **/
    ErrorCode close();

    /**
     * Reserve a set of pages
     * @param in numPages The number of pages to reserve
     * @param out pageId The first reserved pageId
     * @return false if there was an error. true otherwise
     **/
    ErrorCode reserve( const uint32_t& numPages, pageId_t* pageId );

    /**
     * Locks a pages into a buffer
     * @param in data The buffer where the page will be locked
     * @param in pageId The page to lock
     * @return false if the lock was not successful. true otherwise
     * */
    ErrorCode read( char* data, const pageId_t& pageId );

    /**
     * Unlocks the given page
     * @param in data The buffer where the page was locked
     * @param in pageId The page to unlock
     * @return false if the unlock was unsuccessful. true otherwise.
     **/
    ErrorCode write( const char* data, const pageId_t& pageId );

    /**
     * Gets the current size of the storage in pages
     * @return The current size of the storage in pages
     **/
    uint64_t size() const noexcept;

    /**
     * Gets the configuration of the storage
     * @return The configuration of the storage
     * */
    const FileStorageConfig& config() const noexcept;

    /**
     * Gets the page size in bytes
     *
     * @return The page size in bytes
     **/
    uint32_t getPageSize() const noexcept;

  private:

    /**
     * Converts a position in a file in bytes to their pageId counterpart
     * where this byte belongs to
     * @param bytes in bytes The bytes to convert
     * @return The pageId
     **/
    pageId_t bytesToPage( const uint64_t& bytes ) const noexcept;

    /**
     * Converts a page to its equivalent position in bytes
     * @param in pageId The page to convert
     * @return the position in bytes equivalent to the page.
     */
    uint64_t pageToBytes( const pageId_t& pageId ) const noexcept;


    // The data file
    std::fstream    m_dataFile;

    // The configuration file
    std::fstream    m_configFile;

    // The size of the file in pages;
    pageId_t      m_size;

    // The basic file modes
    std::ios_base::openmode m_flags;

    // A buffer used to initialize new pages in the file when it grows
    std::vector<char>       m_pageFiller;

    // The storage configuration data
    FileStorageConfig  m_config;
};

SMILE_NS_END

#endif /* ifndef _STORAGE_FILE_STORAGE_H_ */

