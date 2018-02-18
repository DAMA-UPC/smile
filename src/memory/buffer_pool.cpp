

#include "buffer_pool.h"

SMILE_NS_BEGIN

BufferPool::BufferPool(FileStorage* storage, uint64_t poolSize) noexcept {
	m_storage = storage;
	uint32_t extentSize = m_storage->getExtentSize();
	m_pool = (char*) malloc( poolSize*extentSize*sizeof(char) );
	m_descriptors.resize(poolSize);
	m_bufferSize = extentSize;
	m_allocationTable.resize(poolSize);
	m_nextCSVictim = 0;
}

transactionId_t BufferPool::beginTransaction() {

}

void BufferPool::commitTransaction( const transactionId_t tId ) {

}

bufferId_t BufferPool::alloc( const transactionId_t tId, const extentId_t extentId  ) noexcept {
	bool found = false;
	bufferId_t bufferId;

	// Look for an empty Buffer Pool slot
	for (int i = 0; i < m_allocationTable.size() && !found; ++i) {
		if (!m_allocationTable.test(i)) {
			m_allocationTable.set(i);
			bufferId = i;
			found = true;
		}
	}

	// If there is no empty slot, use Clock Sweep algorithm to find a victim.
	while (!found) {
		uint64_t usageCount = m_descriptors[m_nextCSVictim].usageCount;
		if ( usageCount == 0 ) {
			bufferId = m_nextCSVictim;
			found = true;

			// If the buffer is dirty we must store it to disk
			if( m_descriptors[bufferId].dirty ) {
				char* data = m_pool + (m_bufferSize*bufferId);
				m_storage->write(data, m_descriptors[bufferId].extentId);
			}
		}
		else {
			--m_descriptors[m_nextCSVictim].usageCount;
		}
		m_nextCSVictim = (m_nextCSVictim+1) % m_descriptors.size();
	}

	// Fill the buffer descriptor
	m_descriptors[bufferId].usageCount = 0;
	m_descriptors[bufferId].dirty = 0;
	m_descriptors[bufferId].extentId = extentId;

	// Load page into Buffer Pool
	char* dest = m_pool + (m_bufferSize*bufferId);
	m_storage->read(dest, extentId);

	return bufferId;
}

void BufferPool::release( const bufferId_t bId, const transactionId_t tId ) noexcept {
	// Set buffer as not allocated
	m_allocationTable.set(bId, 0);

	// If the buffer is dirty we must store it to disk
	if( m_descriptors[bId].dirty ) {
		char* data = m_pool + (m_bufferSize*bId);
		m_storage->write(data, m_descriptors[bId].extentId);
	}
}

Buffer BufferPool::pin( const bufferId_t bId, const transactionId_t tId ) noexcept {
	// Increment page's usage count
	++m_descriptors[bId].usageCount;
	
	// Set the handler with corresponding buffer slot
	char* bufferSlot = m_pool + (m_bufferSize*bId);
	Buffer buffer(bId, tId, bufferSlot);

	return buffer;	
}

void BufferPool::unpin( const bufferId_t bId ) noexcept {
	// Decrement page's usage count
	--m_descriptors[bId].usageCount;
}

void BufferPool::checkpoint() noexcept {

}

SMILE_NS_END

