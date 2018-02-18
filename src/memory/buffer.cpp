


#include "buffer.h"

SMILE_NS_BEGIN

Buffer::Buffer(bufferId_t bId, transactionId_t tId, char* bufferSlot) noexcept {
	m_bId = bId;
	m_tId = tId;
	m_bufferSlot = bufferSlot;
}

void Buffer::write(char* data, uint32_t numBytes, uint32_t startByte ) {

}

void Buffer::read(char* data, uint32_t numBytes, uint32_t startByte ) const  {

}

SMILE_NS_END


