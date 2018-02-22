


#ifndef _MEMORY_BUFFER_POOL_H_
#define _MEMORY_BUFFER_POOL_H_

#include "../base/platform.h"
#include "../storage/file_storage.h"
#include "buffer.h"
#include "types.h"

SMILE_NS_BEGIN

class BufferPool {
  public:
    SMILE_NOT_COPYABLE(BufferPool);
    friend class Buffer;

    BufferPool(FileStorage* storage) noexcept;
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
     * @return The id of the allocated buffer
     **/
    bufferId_t alloc( const transactionId_t tId ) noexcept;

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
     * Checkpoints the BufferPool to the storage
     **/
    void checkpoint() noexcept;

    /**
     * Gets the size of a buffer in KB
     * @return The size of a buffer in KB
     **/
    uint64_t bufferSizeKB() const noexcept;

  private:

    /**
     * The file storage where this buffer pool will be persisted
     **/
    FileStorage* const m_storage;

};

SMILE_NS_END

#endif /* ifndef _MEMORY_BUFFER_POOL_H_*/

