
#ifndef _STORAGE_FILE_STORAGE_H_
#define _STORAGE_FILE_STORAGE_H_ 

#include "../base/base.h"
#include "types.h"
#include <vector>

#include <fstream>

SMILE_NS_BEGIN

struct FileStorageConfig {
  uint32_t  m_extentSizeKB = 64;
};

class FileStorage {
  public:
    SMILE_NON_COPYABLE(FileStorage)

    FileStorage() noexcept;

    virtual ~FileStorage() noexcept;

    /**
     * Opens the file storage at the given path
     * @param in the path to the storage
     * @return true if the storage was opened correctly
     **/
    ErrorCode open( const std::string& path ) noexcept;

    /**
     * Opens the file storage at the given path
     * @param in the path to the storage to create
     * @return in the configuration of the storage
     **/
    ErrorCode create( const std::string& path, const FileStorageConfig& config, const bool overwrite = false ) noexcept;

    /**
     * Closes the storage
     * @return true if the closing was successful
     **/
    ErrorCode close() noexcept;

    /**
     * Reserve a set of extents
     * @param in numExtents The number of extent to reserve
     * @param out extentId The first reserved extentId
     * @return false if there was an error. true otherwise
     **/
    ErrorCode reserve( const uint32_t numExtents, extentId_t& extents ) noexcept;

    /**
     * Locks an extent into a buffer
     * @param in data The buffer where the extent will be locked
     * @param in extent The extent to lock
     * @return false if the lock was not successful. true otherwise
     * */
    ErrorCode read( char* data, const extentId_t extent ) noexcept;

    /**
     * Unlocks the given extent
     * @param in data The buffer where the extent was locked
     * @param in extent The extent to unlock
     * @param in drity Whether to write the extent to disk or not.
     * @return false if the unlock was unsuccessful. true otherwise.
     **/
    ErrorCode write( const char* data, const extentId_t extent ) noexcept;

    /**
     * Gets the current size of the storage in extents
     * @return The current size of the storage in extents
     **/
    uint64_t size() const noexcept;

    /**
     * Gets the configuration of the storage
     * @return The configuration of the storage
     * */
    const FileStorageConfig& config() const noexcept;

    /**
     * Gets the extent size in bytes
     *
     * @return The extent size in bytes
     **/
    uint32_t getExtentSize() const noexcept;
    
  private:

    /**
     * Converts a position in a file in bytes to their extentId counterpart
     * where this byte belongs to
     * @param bytes in bytes The bytes to convert
     * @return The extentId
     **/
    extentId_t bytesToExtent( uint64_t bytes ) const noexcept;

    /**
     * Converts an extent to its equivalent position in bytes
     * @param in extentId The extent to convert
     * @return the position in bytes equivalent to the extent.
     */
    uint64_t extentToBytes( const extentId_t extent ) const noexcept;


    // The file
    std::fstream    m_file;

    // The size of the file in extents;
    extentId_t      m_size;

    // The basic file modes
    std::ios_base::openmode m_flags;

    // A buffer used to initialize new extents in the file when it grows
    std::vector<char>       m_extentFiller;

    // The storage configuration data
    FileStorageConfig  m_config;
};

SMILE_NS_END

#endif /* ifndef _STORAGE_FILE_STORAGE_H_ */

