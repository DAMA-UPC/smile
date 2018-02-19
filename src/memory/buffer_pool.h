


#ifndef _MEMORY_BUFFER_POOL_H_
#define _MEMORY_BUFFER_POOL_H_

#include "../base/platform.h"
#include "../storage/file_storage.h"
#include "buffer.h"
#include "types.h"
#include "boost/dynamic_bitset.hpp"


SMILE_NS_BEGIN

class BufferPool {

  public:

    SMILE_NON_COPYABLE(BufferPool);
    friend class Buffer;

    // TODO: BP config 
    BufferPool(FileStorage* storage, uint64_t poolSize = 128) noexcept;
    ~BufferPool() noexcept = default;

    /**
     * Allocates a buffer
     * @param The id of the transaction this allocation belongs to
     * @param The pageId_t to be allocated
     * @return The id of the allocated buffer
     **/
    bufferId_t alloc( const transactionId_t tId, const pageId_t pageId ) noexcept;

    /**
     * Releases a buffer
     * @param in bId The buffer to release
     * @param in tId The id of the transaction 
     **/
    void release( const bufferId_t bId ) noexcept;

    /**
     * Pins the buffer
     * @param in bId The buffer to pin
     * @param in tId The id of the transaction
     * @return The handler of the buffer
     **/
    Buffer pin( const bufferId_t bId, const transactionId_t tId ) noexcept;

    /**
     * Unpins the buffer
     * 
     * @param The bId of the buffer to unpin
     */
    void unpin( const bufferId_t bId ) noexcept;

    /**
     * Checkpoints the BufferPool to the storage
     **/
    void checkpoint() noexcept;

  private:

    /**
     * Get a pointer to the specified buffer slot
     * 
     * @param bId bufferId_t of the buffer to get a pointer to
     * @return Pointer to bId buffer 
     */
    char* getBuffer( const bufferId_t bId ) noexcept;

    struct bufferDescriptor {
        uint64_t    m_usageCount    = 0; // Number of times the stored page has been accessed since it was loaded in the slot
        bool        m_dirty         = 0; // Whether the buffer is dirty or not
        pageId_t    m_pageId        = 0; // extenId_t on disk of the loaded page
    };

    /**
     * The file storage where this buffer pool will be persisted
     **/
    FileStorage* p_storage;

    /**
     * Buffer pool
     */
    char* p_pool;

    /**
     * Buffer descriptors (metadata)
     */
    std::vector<bufferDescriptor> m_descriptors;

    /**
     * Size of a Buffer Pool slot in Bytes
     */
    uint64_t m_pageSize;

    /**
     * Bitset representing whether a Buffer Pool slot is allocated (1) or not (0)
     */
    boost::dynamic_bitset<> m_allocationTable;

    /**
     * Next victim to test during Clock Sweep
     */
    uint64_t m_nextCSVictim;
};

SMILE_NS_END

#endif /* ifndef _MEMORY_BUFFER_POOL_H_*/

