

#include "buffer_pool.h"

SMILE_NS_BEGIN

BufferPool::BufferPool(FileStorage* storage, uint64_t poolSize) noexcept {
	m_storage = storage;
	uint32_t pageSize = m_storage->getPageSize();
	m_pool = (char*) malloc( poolSize*pageSize*sizeof(char) );
	m_descriptors.resize(poolSize);
	m_pageSize = pageSize;
	m_allocationTable.resize(poolSize);
	m_nextCSVictim = 0;
}

bufferId_t BufferPool::alloc( const transactionId_t tId, const pageId_t pageId  ) noexcept {
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
				m_storage->write(getBuffer(bufferId), m_descriptors[bufferId].pageId);
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
	m_descriptors[bufferId].pageId = pageId;

	// Load page into Buffer Pool
	// TODO: call find_empty_slot
	//m_storage->read(getBuffer(bufferId), pageId);

	return bufferId;
}

// TODO: remove from disk
void BufferPool::release( const bufferId_t bId ) noexcept {
	// Set buffer as not allocated
	m_allocationTable.set(bId, 0);

	// If the buffer is dirty we must store it to disk
	if( m_descriptors[bId].dirty ) {
		m_storage->write(getBuffer(bId), m_descriptors[bId].pageId);
	}
}

Buffer BufferPool::pin( const bufferId_t bId, const transactionId_t tId ) noexcept {
	// Increment page's usage count
	++m_descriptors[bId].usageCount;
	
	// Return a handler for the corresponding buffer slot
	Buffer buffer(bId, tId, getBuffer(bId), &m_descriptors[bId].dirty);
	return buffer;	
}

void BufferPool::unpin( const bufferId_t bId ) noexcept {
	// Decrement page's usage count
	--m_descriptors[bId].usageCount;
}

void BufferPool::checkpoint() noexcept {

}

char* BufferPool::getBuffer( const bufferId_t bId ) noexcept {
	char* buffer = m_pool + (m_pageSize*bId);
	return buffer;
}

SMILE_NS_END

