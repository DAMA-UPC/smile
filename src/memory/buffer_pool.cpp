

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

BufferHandler BufferPool::alloc() noexcept {
	// Reserve space in disk and get the corresponding pageId_t.
	pageId_t pId;
	p_storage->reserve(1, pId);

	// Get an empty buffer pool slot for the page.
	bufferId_t bId = getEmptySlot();
	m_bufferTable[pId] = bId;

	// Fill the buffer descriptor.
	m_descriptors[bId].m_referenceCount = 1;
	m_descriptors[bId].m_usageCount = 1;
	m_descriptors[bId].m_dirty = 0;
	m_descriptors[bId].m_pageId = pId;

	// Return a BufferHandler of the allocated buffer.
	BufferHandler bufferHandler(getBuffer(bId), bId);
	return bufferHandler;
}

void BufferPool::release( const pageId_t pId ) noexcept {
	// Check if the page is in the Buffer Pool.	
	std::map<pageId_t, bufferId_t>::iterator it;
	it = m_bufferTable.find(pId);
	if (it != m_bufferTable.end()) {
		bufferId_t bId = it->second;

		// Delete page entry from buffer table.
		m_bufferTable.erase(m_descriptors[bId].m_pageId);

		// Set buffer as not allocated
		m_allocationTable.set(bId, 0);

		// If the buffer is dirty we must store it to disk.
		if( m_descriptors[bId].m_dirty ) {
			p_storage->write(getBuffer(bId), m_descriptors[bId].m_pageId);
		}
		
		// Update buffer descriptor.
		m_descriptors[bId].m_referenceCount = 0;
		m_descriptors[bId].m_usageCount = 0;
		m_descriptors[bId].m_dirty = 0;
		m_descriptors[bId].m_pageId = 0;
	}

	// TODO: notify storage to set as free?	
}

BufferHandler BufferPool::pin( const pageId_t pId ) noexcept {
	bufferId_t bId;

	// Look for the desired page in the Buffer Pool.
	std::map<pageId_t, bufferId_t>::iterator it;
	it = m_bufferTable.find(pId);

	// If it is not already there, get an empty slot for the page and load it
	// from disk. Else take the corresponding slot. Update Buffer Descriptor 
	// accordingly.
	if (it == m_bufferTable.end()) {
		bId = getEmptySlot();
		p_storage->read(getBuffer(bId), pId);
		m_bufferTable[pId] = bId;

		m_descriptors[bId].m_referenceCount = 1;
		m_descriptors[bId].m_usageCount = 1;
		m_descriptors[bId].m_dirty = 0;
	}
	else {
		bId = it->second;

		++m_descriptors[bId].m_referenceCount;
		++m_descriptors[bId].m_usageCount;
	}

	// Fill the remaining buffer descriptor fields.
	m_descriptors[bId].m_pageId = pId;
	
	// Return a BufferHandler of the pinned buffer.
	BufferHandler bufferHandler(getBuffer(bId), bId);
	return bufferHandler;	
}

void BufferPool::unpin( const pageId_t pId ) noexcept {
	// Decrement page's reference count.
	std::map<pageId_t, bufferId_t>::iterator it;
	it = m_bufferTable.find(pId);
	if (it != m_bufferTable.end()) {
		bufferId_t bId = it->second;
		--m_descriptors[bId].m_referenceCount;
	}	
}

void BufferPool::checkpoint() noexcept {
	// Look for dirty Buffer Pool slots and flush them to disk.
	for (int bId = 0; bId < m_allocationTable.size(); ++bId) {
		if (m_allocationTable.test(bId) && m_descriptors[bId].m_dirty) {
			p_storage->write(getBuffer(bId), m_descriptors[bId].m_pageId);
			m_descriptors[bId].m_dirty = 0;
		}
	}
}

char* BufferPool::getBuffer( const bufferId_t bId ) noexcept {
	char* buffer = p_pool + (p_storage->getPageSize()*bId);
	return buffer;
}

// TODO: what if a victim is currently referenced?
bufferId_t BufferPool::getEmptySlot() noexcept {
	bufferId_t bId;
	bool found = false;

	// Look for an empty Buffer Pool slot.
	for (int i = 0; i < m_allocationTable.size() && !found; ++i) {
		if (!m_allocationTable.test(i)) {
			m_allocationTable.set(i);
			bId = i;
			found = true;
		}
	}

	// If there is no empty slot, use Clock Sweep algorithm to find a victim.
	while (!found) {
		uint64_t usageCount = m_descriptors[m_nextCSVictim].m_usageCount;
		if ( usageCount == 0 ) {
			bId = m_nextCSVictim;
			found = true;

			// If the buffer is dirty we must store it to disk
			if( m_descriptors[bId].m_dirty ) {
				p_storage->write(getBuffer(bId), m_descriptors[bId].m_pageId);
			}

			// Delete page entry from buffer table.
			m_bufferTable.erase(m_descriptors[bId].m_pageId);
		}
		else {
			--m_descriptors[m_nextCSVictim].m_usageCount;
		}
		m_nextCSVictim = (m_nextCSVictim+1) % m_descriptors.size();
	}

	return bId;
}

SMILE_NS_END

