

#include "buffer_pool.h"

SMILE_NS_BEGIN

BufferPool::BufferPool(FileStorage* storage, uint64_t poolSize, uint64_t bufferSize) noexcept {
	m_storage = storage;
	//m_pool.resize(poolSize);
	m_descriptors.resize(poolSize);
	m_bufferSize = bufferSize;
	m_allocationTable.resize(poolSize);
	m_nextCSVictim = 0;
}

transactionId_t BufferPool::beginTransaction() {

}

void BufferPool::commitTransaction( const transactionId_t tId ) {

}

bufferId_t BufferPool::alloc( const transactionId_t tId ) noexcept {
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
			if( m_descriptors[m_nextCSVictim].dirty ) {
				// TODO
				// Store changes to FileSystem
			}

			// TODO
			// Reset the buffer for a new use
		}
		else {
			m_descriptors[m_nextCSVictim].usageCount = usageCount + 1;
		}
		m_nextCSVictim = (m_nextCSVictim+1) % m_descriptors.size();
	}

	return bufferId;
}

void BufferPool::release( const bufferId_t bId, const transactionId_t tId ) noexcept {
	// Set buffer as not allocated
	m_allocationTable.set(bId, 0);

	// If the buffer is dirty we must store it to disk
	if( m_descriptors[m_nextCSVictim].dirty ) {
		// TODO
		// Store changes to FileSystem
	}

	// TODO
	// Reset the buffer for a new use
}

Buffer BufferPool::pin( const bufferId_t bId, const transactionId_t tId ) noexcept {
	uint64_t usage = m_descriptors[m_nextCSVictim].usageCount;
	
	// TODO
	// Fill a buffer correctly
	Buffer buffer(bId, tId);

	return buffer;	
}

void BufferPool::checkpoint() noexcept {

}

uint64_t BufferPool::bufferSizeKB() const noexcept {
	return m_bufferSize;
}

SMILE_NS_END

