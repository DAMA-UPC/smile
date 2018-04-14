


#ifndef _MEMORY_BUFFER_POOL_H_
#define _MEMORY_BUFFER_POOL_H_

#include <map>
#include <list>
#include <mutex>
#include <shared_mutex>
#include "../base/platform.h"
#include "../storage/file_storage.h"
#include "types.h"
#include "boost/dynamic_bitset.hpp"


SMILE_NS_BEGIN

struct BufferPoolConfig {
    /**
     * Size of the Buffer Pool in KB.
     */
    uint32_t  m_poolSizeKB = 1024*1024;
};

struct BufferHandler {
    /**
     * Pointer to buffer ID from the Buffer Pool.
     */
    char*           m_buffer; 

    /**
     * pageId_t of the page corresponding to the buffer.
     */
    pageId_t        m_pId;

    /**
     * bufferId_t of the buffer accessed by the handler.
     */
    bufferId_t      m_bId;
};

struct BufferDescriptor {
    /**
     * Whether a buffer slot is currently being used or not.
     */
    bool        m_inUse         = false;
    /**
     * Number of current references of the page.
     */
    uint64_t    m_referenceCount = 0;

    /**
     * Number of times the stored page has been accessed since it was loaded in the slot.
     */
    uint64_t    m_usageCount    = 0;

    /**
     * Whether the buffer is dirty or not.
     */
    bool        m_dirty         = false;

    /**
     * pageId_t on disk of the loaded page.
     */
    pageId_t    m_pageId        = 0;

    /**
     * Lock to isolate I/O operations in a same buffer slot.
     */
    std::unique_ptr<std::mutex> m_ioInProgressLock = nullptr;

    /**
     * Lock to isolate descriptor's modifications.
     */
    std::unique_ptr<std::shared_timed_mutex> m_contentLock = nullptr;
};

struct BufferPoolStatistics {
    /**
     * Number of pages currently in Buffer Pool. 
     */
    uint64_t    m_numAllocatedPages;

    /**
     * Number of pages reserved in the storage.
     */
    uint64_t    m_numReservedPages;    

    /**
     * The size of a page in bytes
     */
    uint64_t    m_pageSize;
};

class BufferPool {

  public:
    SMILE_NOT_COPYABLE(BufferPool);

    friend class Buffer;

    BufferPool() noexcept;

    ~BufferPool() noexcept = default;

    /**
     * Opens the Buffer Pool with a file storage at the given path.
     * 
     * @param in the path to the storage.
     * @return false if the storage was opened correctly.
     **/
    ErrorCode open( const BufferPoolConfig& bpConfig, const std::string& path );

    /**
     * Opens the Buffer Pool with a file storage at the given path.
     * 
     * @param in the path to the storage to create.
     * @return in the configuration of the storage.
     **/
    ErrorCode create( const BufferPoolConfig& bpConfig, const std::string& path, const FileStorageConfig& fsConfig, const bool& overwrite = false );

    /**
     * Closes the Buffer Pool.
     * 
     * @return false if the closing was successful.
     **/
    ErrorCode close() noexcept;

    /**
     * Allocates a new page in the Buffer Pool.
     * 
     * @param bufferHandler BufferHandler for the allocated page.
     * @return false if the alloc was successful, true otherwise.
     */
    ErrorCode alloc( BufferHandler* bufferHandler ) noexcept;

    /**
     * Releases a page from the Buffer Pool.
     * 
     * @param pId Page to release.
     * @return false if the release was successful, true otherwise.
     */
    ErrorCode release( const pageId_t& pId ) noexcept;

    /**
     * Pins a page.
     * 
     * @param pId Page to pin.
     * @param bufferHandler BufferHandler for the pinned page.
     * @return false if the pin was successful, true otherwise.
     */
    ErrorCode pin( const pageId_t& pId, BufferHandler* bufferHandler ) noexcept;

    /**
     * Unpins a page.
     * 
     * @param pId Page to unpin.
     * @return false if the unpin was successful, true otherwise.
     */
    ErrorCode unpin( const pageId_t& pId ) noexcept;

    /**
     * Checkpoints the BufferPool to the storage.
     * 
     * @return false if the checkpoint was successful, true otherwise.
     */
    ErrorCode checkpoint() noexcept;

    /**
     * Sets a page as dirty.
     * 
     * @param pId pageId_t of the page to be set as dirty.
     */
    ErrorCode setPageDirty( const pageId_t& pId ) noexcept;

    /**
     * Gets some stats regarding the Buffer Pool's usage.
     * 
     * @param stats The BufferPoolStatistics to fill with stats.
     * @return false if stats were correctly retrieved, true ottherwise.
     */
    ErrorCode getStatistics( BufferPoolStatistics* stats ) noexcept;

    /**
     * Checks Buffer Pool's consistency 
     * 
     * @return false if Buffer Pool is in a consistent state, true ottherwise.
     */
    ErrorCode checkConsistency();

  private:

    /**
     * Get a pointer to the specified buffer slot.
     * 
     * @param bId bufferId_t of the buffer to get a pointer to.
     * @return Pointer to bId buffer.
     */
    char* getBuffer( const bufferId_t& bId ) noexcept;

    /**
     * Returns the bufferId_t of an empty buffer pool slot. In case none is free
     * Clock Sweep algorithm is performed to evict a page.
     * 
     * @param bId bufferId_t of the free pool slot.
     * @return false if all pages are pinned, true otherwise.
     */
    ErrorCode getEmptySlot( bufferId_t* bId );

    /**
     * Reserve a set of pages and manage the increments of the allocation table.
     * 
     * @param numPages The number of pages to reserve
     * @param pageId The first reserved pageId
     * @return false if there was an error, true otherwise
     */
    ErrorCode reservePages( const uint32_t& numPages, pageId_t* pageId ) noexcept;

    /**
     * Loads the allocation table from disk.
     * 
     * @return false if table is loaded without issues, true otherwise.
     */
    ErrorCode loadAllocationTable() noexcept;

    /**
     * Stores the allocation table in disk.
     * 
     * @return false if the table is stored without issues, true otherwise.
     */
    ErrorCode storeAllocationTable() noexcept;

    /**
     * Returns whether a page is protected (used for storing DB state) or not.
     * 
     * @param pId pageId_t of the page to check.
     * @return true if the page is protected, false otherwise.
     */
    bool isProtected( const pageId_t& pId ) noexcept;

    /**
     * Flushes dirty buffers back to disk.
     * 
     * @return false if buffers have been correctly flushed, true otherwise
     */
    ErrorCode flushDirtyBuffers() noexcept;

    /**
     * The file storage where this buffer pool will be persisted.
     **/
    FileStorage m_storage;

    /**
     * The Buffer Pool configuration data.
     */
    BufferPoolConfig m_config;

    /**
     * Buffer pool.
     */
    char* p_pool;

    /**
     * Buffer descriptors (metadata).
     */
    std::vector<BufferDescriptor> m_descriptors;

    /**
     * Bitset representing whether a disk page is allocated (1) or not (0).
     */
    boost::dynamic_bitset<> m_allocationTable;

    /**
     * List of free (unallocated) pages.
     */
    std::list<pageId_t> m_freePages;

    /**
     * Maps pageId_t with its bufferId_t in case it is currently in the Buffer Pool. 
     */
    std::map<pageId_t, bufferId_t> m_bufferToPageMap;

    /**
     * Next victim to test during Clock Sweep.
     */
    uint64_t m_nextCSVictim;

    /**
     * Global lock to isolate concurrent operations by different threads.
     */
    mutable std::shared_timed_mutex m_globalLock;
};

SMILE_NS_END

#endif /* ifndef _MEMORY_BUFFER_POOL_H_*/

