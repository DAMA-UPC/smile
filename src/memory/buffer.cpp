


#include <cstring>
#include "buffer.h"

SMILE_NS_BEGIN

Buffer::Buffer( bufferId_t bId, transactionId_t tId, char* bufferSlot, bool& dirty ) noexcept {
	m_bId = bId;
	m_tId = tId;
	m_bufferSlot = bufferSlot;
	m_dirty = dirty;
}

void Buffer::write( char* data, uint32_t numBytes, uint32_t startByte ) {
	char* dest = m_bufferSlot + startByte;
	memcpy(dest, data, numBytes);

	// Set dirty bit;
	m_dirty = true;
}

void Buffer::read( char* data, uint32_t numBytes, uint32_t startByte ) const  {
	char* orig = m_bufferSlot + startByte;
	memcpy(data, orig, numBytes);
}

SMILE_NS_END


