


#include <cstring>
#include "buffer.h"

SMILE_NS_BEGIN

Buffer::Buffer(bufferId_t bId, transactionId_t tId, char* bufferSlot) noexcept {
	m_bId = bId;
	m_tId = tId;
	m_bufferSlot = bufferSlot;
}

void Buffer::write(char* data, uint32_t numBytes, uint32_t startByte ) {
	char* dest = m_bufferSlot + startByte;
	memcpy(dest, data, numBytes);
}

void Buffer::read(char* data, uint32_t numBytes, uint32_t startByte ) const  {
	char* orig = m_bufferSlot + startByte;
	memcpy(data, orig, numBytes);
}

SMILE_NS_END


