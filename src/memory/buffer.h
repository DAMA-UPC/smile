


#ifndef _SMILE_MEMORY_BUFFER_H_
#define _SMILE_MEMORY_BUFFER_H_ 

#include "types.h"

SMILE_NS_BEGIN

class BufferPool;

class Buffer {

  public:

    Buffer( bufferId_t bId, transactionId_t tId, char* bufferSlot, bool& dirty ) noexcept;
    
    ~Buffer() noexcept = default;

    /**
     * Write data into buffer
     * 
     * @param data Information to be stored into the buffer
     * @param numBytes Number of bytes to be copied
     * @param startByte Position of the buffer where the write operation starts
     */
    void write( char* data, uint32_t numBytes, uint32_t startByte );

    /**
     * Read data from buffer
     * 
     * @param data Where the data read will be stored
     * @param numBytes Number of bytes to be read
     * @param startByte Position of the buffer where the read operation starts
     */
    void read( char* data, uint32_t numBytes, uint32_t startByte ) const ;

  private:

    /**
     * ???
     */
    char*           m_shadow;

    /**
     * ID of the buffer accessed by the handler
     */
    bufferId_t      m_bId;

    /**
     * ID of the transaction using the handler
     */
    transactionId_t m_tId;

    /**
     * Pointer to buffer ID from the Buffer Pool
     */
    char*           m_bufferSlot; 

    /**
     * Dirty bit
     */
    bool            m_dirty;
};

SMILE_NS_END

#endif /* ifndef _SMILE_MEMORY_BUFFER_H_ */
