

#include "buffer_pool.h"

SMILE_NS_BEGIN

BufferPool::BufferPool( FileStorage* storage, const BufferPoolConfig& config ) noexcept {
	p_storage = storage;
	p_pool = (char*) malloc( config.m_poolSizeKB*1024*sizeof(char) );

	uint32_t pageSizeKB = p_storage->getPageSize() / 1024;
	uint32_t poolElems = config.m_poolSizeKB / pageSizeKB;
	m_descriptors.resize(poolElems);
	m_nextCSVictim = 0;
}

ErrorCode BufferPool::alloc( BufferHandler* bufferHandler ) noexcept {
	// Get an empty buffer pool slot for the page.
	bufferId_t bId;
	ErrorCode error = getEmptySlot(&bId);
	if ( error != ErrorCode::E_NO_ERROR ) {
		return error;
	}

	// Reserve space in disk and get the corresponding pageId_t.
	pageId_t pId;
	error = p_storage->reserve(1, &pId);
	if ( error != ErrorCode::E_NO_ERROR ) {
		return error;
	}

	m_bufferToPageMap[pId] = bId;

	// Fill the buffer descriptor.
	m_descriptors[bId].m_referenceCount = 1;
	m_descriptors[bId].m_usageCount = 1;
	m_descriptors[bId].m_dirty = 0;
	m_descriptors[bId].m_pageId = pId;

	// Set BufferHandler for the allocated buffer.
	bufferHandler->m_buffer 	= getBuffer(bId);
	bufferHandler->m_pId 		= pId;
	bufferHandler->m_bId 		= bId;

	return ErrorCode::E_NO_ERROR;
}

ErrorCode BufferPool::release( const pageId_t& pId ) noexcept {
	// Check if the page is in the Buffer Pool.	
	std::map<pageId_t, bufferId_t>::iterator it;
	it = m_bufferToPageMap.find(pId);
	if (it != m_bufferToPageMap.end()) {
		bufferId_t bId = it->second;

		// Delete page entry from buffer table.
		m_bufferToPageMap.erase(m_descriptors[bId].m_pageId);

		// Set buffer as not used
		m_descriptors[bId].m_inUse = false;

		// If the buffer is dirty we must store it to disk.
		if( m_descriptors[bId].m_dirty ) {
			ErrorCode error = p_storage->write(getBuffer(bId), m_descriptors[bId].m_pageId);
			if ( error != ErrorCode::E_NO_ERROR ) {
				return error;
			}
		}
		
		// Update buffer descriptor.
		m_descriptors[bId].m_referenceCount = 0;
		m_descriptors[bId].m_usageCount = 0;
		m_descriptors[bId].m_dirty = 0;
		m_descriptors[bId].m_pageId = 0;
	}
	else {
		return ErrorCode::E_BUFPOOL_PAGE_NOT_PRESENT;
	}

	// TODO: notify storage to set as free?

	return ErrorCode::E_NO_ERROR;
}

ErrorCode BufferPool::pin( const pageId_t& pId, BufferHandler* bufferHandler ) noexcept {
	bufferId_t bId;

	// Look for the desired page in the Buffer Pool.
	std::map<pageId_t, bufferId_t>::iterator it;
	it = m_bufferToPageMap.find(pId);

	// If it is not already there, get an empty slot for the page and load it
	// from disk. Else take the corresponding slot. Update Buffer Descriptor 
	// accordingly.
	if (it == m_bufferToPageMap.end()) {
		ErrorCode error = getEmptySlot(&bId);
		if ( error != ErrorCode::E_NO_ERROR ) {
			return error;
		}

		error = p_storage->read(getBuffer(bId), pId);
		if ( error != ErrorCode::E_NO_ERROR ) {
			return error;
		}

		m_bufferToPageMap[pId] = bId;

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
	
	// Set BufferHandler for the pinned buffer.
	bufferHandler->m_buffer 	= getBuffer(bId);
	bufferHandler->m_pId 	= pId;
	bufferHandler->m_bId 	= bId;
	
	return ErrorCode::E_NO_ERROR;
}

ErrorCode BufferPool::unpin( const pageId_t& pId ) noexcept {
	// Decrement page's reference count.
	std::map<pageId_t, bufferId_t>::iterator it;
	it = m_bufferToPageMap.find(pId);
	if (it != m_bufferToPageMap.end()) {
		bufferId_t bId = it->second;
		--m_descriptors[bId].m_referenceCount;
	}
	else {
		return ErrorCode::E_BUFPOOL_PAGE_NOT_PRESENT;
	}	

	return ErrorCode::E_NO_ERROR;
}

ErrorCode BufferPool::checkpoint() noexcept {
	// Look for dirty Buffer Pool slots and flush them to disk.
	for (int bId = 0; bId < m_descriptors.size(); ++bId) {
		if (m_descriptors[bId].m_inUse && m_descriptors[bId].m_dirty) {
			ErrorCode error = p_storage->write(getBuffer(bId), m_descriptors[bId].m_pageId);
			if ( error != ErrorCode::E_NO_ERROR ) {
				return error;
			}

			m_descriptors[bId].m_dirty = 0;
		}
	}

	return ErrorCode::E_NO_ERROR;
}

void BufferPool::setPageDirty( const pageId_t& pId ) noexcept {
	std::map<pageId_t, bufferId_t>::iterator it;
	it = m_bufferToPageMap.find(pId);
	if (it != m_bufferToPageMap.end()) {
		bufferId_t bId = it->second;
		m_descriptors[bId].m_dirty = 1;
	}
}

char* BufferPool::getBuffer( const bufferId_t& bId ) noexcept {
	char* buffer = p_pool + (p_storage->getPageSize()*bId);
	return buffer;
}

ErrorCode BufferPool::getEmptySlot( bufferId_t* bId ) noexcept {
	bool found = false;

	// Look for an empty Buffer Pool slot.
	for (int i = 0; i < m_descriptors.size() && !found; ++i) {
		if (!m_descriptors[i].m_inUse) {
			m_descriptors[i].m_inUse = true;
			*bId = i;
			found = true;
		}
	}

	bool existUnpinnedPage = false;
	uint64_t start = m_nextCSVictim;

	// If there is no empty slot, use Clock Sweep algorithm to find a victim.
	while (!found) {
		// Check only unpinned pages
		if (m_descriptors[m_nextCSVictim].m_referenceCount == 0)
		{
			existUnpinnedPage = true;

			if ( m_descriptors[m_nextCSVictim].m_usageCount == 0 ) {
				*bId = m_nextCSVictim;
				found = true;

				// If the buffer is dirty we must store it to disk.
				if( m_descriptors[*bId].m_dirty ) {
					ErrorCode error = p_storage->write(getBuffer(*bId), m_descriptors[*bId].m_pageId);
					if ( error != ErrorCode::E_NO_ERROR ) {
						return error;
					}
				}

				// Delete page entry from buffer table.
				m_bufferToPageMap.erase(m_descriptors[*bId].m_pageId);
			}
			else {
				--m_descriptors[m_nextCSVictim].m_usageCount;
			}	
		}
		
		// Advance victim pointer.
		m_nextCSVictim = (m_nextCSVictim+1) % m_descriptors.size();

		// Stop Clock Sweep in case there are no unpinned pages.
		if (!existUnpinnedPage && m_nextCSVictim == start) {
			break;
		}
	}

	if (!found)	{
		return ErrorCode::E_BUFPOOL_OUT_OF_MEMORY;
	}

	return ErrorCode::E_NO_ERROR;
}

SMILE_NS_END

