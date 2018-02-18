


#ifndef _SMILE_MEMORY_BUFFER_H_
#define _SMILE_MEMORY_BUFFER_H_ 

#include "types.h"

SMILE_NS_BEGIN

class BufferPool;

class Buffer {
  public:
    Buffer(bufferId_t bId, transactionId_t tId, char* bufferSlot) noexcept;
    ~Buffer() noexcept;

    void write(char* data, uint32_t numBytes, uint32_t startByte );

    void read(char* data, uint32_t numBytes, uint32_t startByte ) const ;

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
};

SMILE_NS_END

#endif /* ifndef _SMILE_MEMORY_BUFFER_H_ */
