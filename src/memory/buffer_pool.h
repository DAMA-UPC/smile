


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

    BufferPool(FileStorage* storage, uint64_t poolSize = 128) noexcept;
    ~BufferPool() noexcept = default;

    /**
     * Starts a transaction
     * @return The id of the transaction
     **/
    transactionId_t beginTransaction();

    /**
     * Commits a transaction
     * @param in tId The id of the transaction to commit
     **/
    void commitTransaction( const transactionId_t tId );

    /**
     * Allocates a buffer
     * @param The id of the transaction this allocation belongs to
     * @param The extentId_t to be allocated
     * @return The id of the allocated buffer
     **/
    bufferId_t alloc( const transactionId_t tId, const extentId_t extentId ) noexcept;

    /**
     * Releases a buffer
     * @param in bId The buffer to release
     * @param in tId The id of the transaction 
     **/
    void release( const bufferId_t bId, const transactionId_t tId ) noexcept;

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

    struct bufferDescriptor {
        uint64_t    usageCount; // Number of times the stored page has been accessed since it was loaded in the slot
        bool        dirty;      // Whether the buffer is dirty or not
        extentId_t  extentId;   // extenId_t on disk of the loaded page
    };

    /**
     * The file storage where this buffer pool will be persisted
     **/
    FileStorage* m_storage;

    /**
     * Buffer pool
     */
    char* m_pool;

    /**
     * Buffer descriptors (metadata)
     */
    std::vector<bufferDescriptor> m_descriptors;

    /**
     * Size of a Buffer Pool slot in KB
     */
    uint64_t m_bufferSize;

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

