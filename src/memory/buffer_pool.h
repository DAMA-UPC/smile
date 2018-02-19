


#ifndef _MEMORY_BUFFER_POOL_H_
#define _MEMORY_BUFFER_POOL_H_

#include <map>
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
     * Struct constructor
     */
    BufferHandler(char* buffer, bufferId_t bId) {
        char*           m_buffer = buffer; 
        bufferId_t      m_bId = bId;
    }

    /**
     * Pointer to buffer ID from the Buffer Pool.
     */
    char*           m_buffer; 

    /**
     * ID of the buffer accessed by the handler.
     */
    bufferId_t      m_bId;
};

class BufferPool {

  public:

    SMILE_NON_COPYABLE(BufferPool);

    friend class Buffer;

    BufferPool(FileStorage* storage, const BufferPoolConfig& config) noexcept;

    ~BufferPool() noexcept = default;

    /**
     * Allocates a new page in the Buffer Pool.
     * 
     * @return BufferHandler for the allocated page.
     */
    BufferHandler alloc() noexcept;

    /**
     * Releases a page from the Buffer Pool.
     * 
     * @param pId Page to release.
     */
    void release( const pageId_t pId ) noexcept;

    /**
     * Pins a page.
     * 
     * @param pId Page to pin.
     * @return BufferHandler for the pinned page.
     */
    BufferHandler pin( const pageId_t pId ) noexcept;

    /**
     * Unpins a page.
     * 
     * @param pId Page to unpin.
     */
    void unpin( const pageId_t pId ) noexcept;

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

    /**
     * Returns the bufferId_t of an empty buffer pool slot. In case none is free
     * Clock Sweep algorithm is performed to evict a page.
     * 
     * @return bufferId_t of the free pool slot.
     */
    bufferId_t getEmptySlot() noexcept;

    struct bufferDescriptor {
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
        bool        m_dirty         = 0;

        /**
         * pageId_t on disk of the loaded page.
         */
        pageId_t    m_pageId        = 0;
    };

    /**
     * The file storage where this buffer pool will be persisted.
     **/
    FileStorage* p_storage;

    /**
     * Buffer pool.
     */
    char* p_pool;

    /**
     * Buffer descriptors (metadata).
     */
    std::vector<bufferDescriptor> m_descriptors;

    /**
     * Bitset representing whether a Buffer Pool slot is allocated (1) or not (0).
     */
    boost::dynamic_bitset<> m_allocationTable;

    /**
     * Maps pageId_t with its bufferId_t in case it is currently in the Buffer Pool. 
     */
    std::map<pageId_t, bufferId_t> m_bufferTable;

    /**
     * Next victim to test during Clock Sweep.
     */
    uint64_t m_nextCSVictim;
};

SMILE_NS_END

#endif /* ifndef _MEMORY_BUFFER_POOL_H_*/

