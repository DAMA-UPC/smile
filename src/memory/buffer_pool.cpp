

#include "buffer_pool.h"

SMILE_NS_BEGIN

BufferPool::BufferPool(FileStorage* storage, const BufferPoolConfig& config) noexcept {
	p_storage = storage;
	p_pool = (char*) malloc( config.m_poolSizeKB*1024*sizeof(char) );

	uint32_t pageSizeKB = p_storage->getPageSize() / 1024;
	uint32_t poolElems = config.m_poolSizeKB / pageSizeKB;
	m_descriptors.resize(poolElems);
	m_allocationTable.resize(poolElems);
	m_nextCSVictim = 0;
}

bufferId_t BufferPool::alloc( const transactionId_t tId, const pageId_t pageId  ) noexcept {
	bufferId_t bId = getEmptySlot();	

	// Fill the buffer descriptor
	m_descriptors[bId].m_usageCount = 0;
	m_descriptors[bId].m_dirty = 0;
	m_descriptors[bId].m_pageId = pageId;

	return bId;
}

// TODO: remove from disk
void BufferPool::release( const bufferId_t bId ) noexcept {
	// Set buffer as not allocated
	m_allocationTable.set(bId, 0);

	// If the buffer is dirty we must store it to disk
	if( m_descriptors[bId].m_dirty ) {
		p_storage->write(getBuffer(bId), m_descriptors[bId].m_pageId);
	}
}

Buffer BufferPool::pin( const bufferId_t bId, const transactionId_t tId ) noexcept {
	// Increment page's usage count
	++m_descriptors[bId].m_usageCount;
	
	// Return a handler for the corresponding buffer slot
	Buffer buffer(bId, tId, getBuffer(bId), &m_descriptors[bId].m_dirty);
	return buffer;	
}

void BufferPool::unpin( const bufferId_t bId ) noexcept {
	// Decrement page's usage count
	--m_descriptors[bId].m_usageCount;
}

void BufferPool::checkpoint() noexcept {

}

char* BufferPool::getBuffer( const bufferId_t bId ) noexcept {
	char* buffer = p_pool + (p_storage->getPageSize()*bId);
	return buffer;
}

bufferId_t BufferPool::getEmptySlot() noexcept {
	bufferId_t bufferId;
	bool found = false;

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
		uint64_t usageCount = m_descriptors[m_nextCSVictim].m_usageCount;
		if ( usageCount == 0 ) {
			bufferId = m_nextCSVictim;
			found = true;

			// If the buffer is dirty we must store it to disk
			if( m_descriptors[bufferId].m_dirty ) {
				p_storage->write(getBuffer(bufferId), m_descriptors[bufferId].m_pageId);
			}
		}
		else {
			--m_descriptors[m_nextCSVictim].m_usageCount;
		}
		m_nextCSVictim = (m_nextCSVictim+1) % m_descriptors.size();
	}

	return bufferId;
}
SMILE_NS_END

