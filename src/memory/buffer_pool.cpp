

#include "buffer_pool.h"

SMILE_NS_BEGIN

BufferPool::BufferPool() noexcept {	
}

ErrorCode BufferPool::open( const BufferPoolConfig& bpConfig, const std::string& path ) noexcept {
	std::lock_guard<std::mutex> lock(m_globalLoc);

	if ( bpConfig.m_poolSizeKB % (m_storage.getPageSize()/1024) != 0 ) {
		return ErrorCode::E_BUFPOOL_POOL_SIZE_NOT_MULTIPLE_OF_PAGE_SIZE;
	}

	ErrorCode error = m_storage.open(path);
	if ( error != ErrorCode::E_NO_ERROR ) {
		return error;
	}

	m_config = bpConfig;
	p_pool = (char*) malloc( m_config.m_poolSizeKB*1024*sizeof(char) );
	uint32_t pageSizeKB = m_storage.getPageSize() / 1024;
	uint32_t poolElems = m_config.m_poolSizeKB / pageSizeKB;
	m_descriptors.resize(poolElems);
	m_nextCSVictim = 0;

	error = loadAllocationTable();
	if ( error != ErrorCode::E_NO_ERROR ) {
		return error;
	}

	return ErrorCode::E_NO_ERROR;
}

ErrorCode BufferPool::create( const BufferPoolConfig& bpConfig, const std::string& path, const FileStorageConfig& fsConfig, const bool& overwrite ) noexcept {
	std::lock_guard<std::mutex> lock(m_globalLoc);

	if ( bpConfig.m_poolSizeKB % fsConfig.m_pageSizeKB != 0 ) {
		return ErrorCode::E_BUFPOOL_POOL_SIZE_NOT_MULTIPLE_OF_PAGE_SIZE;
	}

	ErrorCode error = m_storage.create(path, fsConfig, overwrite);
	if ( error != ErrorCode::E_NO_ERROR ) {
		return error;
	}

	m_config = bpConfig;
	p_pool = (char*) malloc( m_config.m_poolSizeKB*1024*sizeof(char) );
	uint32_t pageSizeKB = m_storage.getPageSize() / 1024;
	uint32_t poolElems = m_config.m_poolSizeKB / pageSizeKB;
	m_descriptors.resize(poolElems);
	m_nextCSVictim = 0;

	return ErrorCode::E_NO_ERROR;
}

ErrorCode BufferPool::close() noexcept {
	std::lock_guard<std::mutex> lock(m_globalLoc);

	// Flush dirty buffers
	ErrorCode error = flushDirtyBuffers();
	if ( error != ErrorCode::E_NO_ERROR ) {
		return error;
	}

	// Save m_allocationTable state to disk.
	error = storeAllocationTable();
	if ( error != ErrorCode::E_NO_ERROR ) {
		return error;
	}

	error = m_storage.close();
	if ( error != ErrorCode::E_NO_ERROR ) {
		return error;
	}

	free(p_pool);
	m_descriptors.clear();
	m_allocationTable.clear();
	m_freePages.clear();
	m_bufferToPageMap.clear();

	return ErrorCode::E_NO_ERROR;
}

ErrorCode BufferPool::alloc( BufferHandler* bufferHandler ) noexcept {
	std::lock_guard<std::mutex> lock(m_globalLoc);

	bufferId_t bId;
	pageId_t pId;

	// Get an empty buffer pool slot for the page.
	ErrorCode error = getEmptySlot(&bId);
	if ( error != ErrorCode::E_NO_ERROR ) {
		return error;
	}

	// If there is no more free space in disk, reserve space.
	if (m_freePages.empty()) {
		error = reservePages(1, &pId);
		if ( error != ErrorCode::E_NO_ERROR ) {
			return error;
		}
	}

	// Set page as allocated.
	pId = m_freePages.front();
	m_freePages.pop_front();
	m_allocationTable.set(pId);

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
	std::lock_guard<std::mutex> lock(m_globalLoc);

	if (pId > m_storage.size()) {
		return ErrorCode::E_BUFPOOL_PAGE_NOT_ALLOCATED;
	}

	if (isProtected(pId)) {
		return ErrorCode::E_BUFPOOL_UNABLE_TO_ACCCESS_PROTECTED_PAGE;	
	}

	// Evict the page in case it is in the Buffer Pool
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
			ErrorCode error = m_storage.write(getBuffer(bId), m_descriptors[bId].m_pageId);
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
	
	// Set page as unallocated.
	m_allocationTable.set(pId, 0);
	m_freePages.push_back(pId);

	return ErrorCode::E_NO_ERROR;
}

ErrorCode BufferPool::pin( const pageId_t& pId, BufferHandler* bufferHandler ) noexcept {
	std::lock_guard<std::mutex> lock(m_globalLoc);

	if (pId > m_storage.size()) {
		return ErrorCode::E_BUFPOOL_PAGE_NOT_ALLOCATED;
	}

	if (isProtected(pId)) {
		return ErrorCode::E_BUFPOOL_UNABLE_TO_ACCCESS_PROTECTED_PAGE;	
	}

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

		error = m_storage.read(getBuffer(bId), pId);
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
	std::lock_guard<std::mutex> lock(m_globalLoc);

	if (pId > m_storage.size()) {
		return ErrorCode::E_BUFPOOL_PAGE_NOT_ALLOCATED;
	}

	if (isProtected(pId)) {
		return ErrorCode::E_BUFPOOL_UNABLE_TO_ACCCESS_PROTECTED_PAGE;	
	}

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
	std::lock_guard<std::mutex> lock(m_globalLoc);

	// Flush dirty buffers
	ErrorCode error = flushDirtyBuffers();
	if ( error != ErrorCode::E_NO_ERROR ) {
		return error;
	}

	// Save m_allocationTable state to disk.
	error = storeAllocationTable();
	if ( error != ErrorCode::E_NO_ERROR ) {
		return error;
	}

	return ErrorCode::E_NO_ERROR;
}

ErrorCode BufferPool::setPageDirty( const pageId_t& pId ) noexcept {
	std::lock_guard<std::mutex> lock(m_globalLoc);

	std::map<pageId_t, bufferId_t>::iterator it;
	it = m_bufferToPageMap.find(pId);
	if (it != m_bufferToPageMap.end()) {
		bufferId_t bId = it->second;
		m_descriptors[bId].m_dirty = 1;
	}
	else {
		return ErrorCode::E_BUFPOOL_PAGE_NOT_PRESENT;
	}

	return ErrorCode::E_NO_ERROR;
}

ErrorCode BufferPool::getStatistics( BufferPoolStatistics* stats ) noexcept {
	std::lock_guard<std::mutex> lock(m_globalLoc);

	uint64_t numAllocatedPages = 0;
	for (uint64_t i = 0; i < m_descriptors.size(); ++i) {
		if (m_descriptors[i].m_inUse) {
			++numAllocatedPages;
		}
	}

	stats->m_numAllocatedPages = numAllocatedPages;
	stats->m_numReservedPages = m_storage.size();

	return ErrorCode::E_NO_ERROR;
}

ErrorCode BufferPool::checkConsistency() noexcept {
	std::lock_guard<std::mutex> lock(m_globalLoc);

	for (uint64_t i = 0; i < m_allocationTable.size(); ++i) {
		std::list<pageId_t>::iterator itFreePages = std::find(m_freePages.begin(), m_freePages.end(), i);
		std::map<pageId_t, bufferId_t>::iterator itBuffer = m_bufferToPageMap.find(reinterpret_cast<pageId_t>(i));

		if (m_allocationTable.test(i)) {
			if (!isProtected(i) && itFreePages != m_freePages.end()) {
				return ErrorCode::E_BUFPOOL_ALLOCATED_PAGE_IN_FREELIST;
			}
			else if (isProtected(i) && itFreePages != m_freePages.end()) {
				return ErrorCode::E_BUFPOOL_PROTECTED_PAGE_IN_FREELIST;
			}
			
			if (itBuffer != m_bufferToPageMap.end()) {
				if (!m_descriptors[itBuffer->second].m_inUse || !(m_descriptors[itBuffer->second].m_pageId == i)) {
					return ErrorCode::E_BUFPOOL_BUFFER_DESCRIPTOR_INCORRECT_DATA;
				}
			}
		}
		else {
			if (!isProtected(i) && itFreePages == m_freePages.end()) {
				return ErrorCode::E_BUFPOOL_FREE_PAGE_NOT_IN_FREELIST;
			}

			if (itBuffer != m_bufferToPageMap.end()) {
				return ErrorCode::E_BUFPOOL_FREE_PAGE_MAPPED_TO_BUFFER;
			}
		}
	}

	return ErrorCode::E_NO_ERROR;
}

char* BufferPool::getBuffer( const bufferId_t& bId ) noexcept {
	char* buffer = p_pool + (m_storage.getPageSize()*bId);
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
					ErrorCode error = m_storage.write(getBuffer(*bId), m_descriptors[*bId].m_pageId);
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

ErrorCode BufferPool::reservePages( const uint32_t& numPages, pageId_t* pageId ) noexcept {
	// Reserve space in disk.
	ErrorCode error = m_storage.reserve(numPages, pageId);
	if ( error != ErrorCode::E_NO_ERROR ) {
		return error;
	}

	// Add reserved pages to free list and increment the Allocation Table size to fit the new pages.
	for (uint64_t i = 0; i < numPages; ++i) {
		m_freePages.push_back((*pageId)+i);
		m_allocationTable.push_back(0);
	}

	// If the first reserved page is protected, it must be removed from the free list and
	// an extra page must be reserved.
	if (isProtected(*pageId)) {
		m_freePages.remove(*pageId);

		pageId_t pIdToReturn = (*pageId) + 1;

		ErrorCode error = m_storage.reserve(1, pageId);
		if ( error != ErrorCode::E_NO_ERROR ) {
			return error;
		}
		m_freePages.push_back(*pageId);
		m_allocationTable.push_back(0);

		*pageId = pIdToReturn;
	}

	return ErrorCode::E_NO_ERROR;
}

ErrorCode BufferPool::loadAllocationTable() noexcept {
	uint64_t numTotalPages = m_storage.size();
	uint64_t bitsPerPage = 8*m_storage.getPageSize();
	uint64_t numBitmapPages = numTotalPages/bitsPerPage + (numTotalPages%bitsPerPage != 0);
	uint64_t bitmapSize = numBitmapPages*bitsPerPage;
	uint64_t blockSize = boost::dynamic_bitset<>::bits_per_block;

	std::vector<boost::dynamic_bitset<>::block_type> v(bitmapSize/blockSize);

	for (uint64_t i = 0; i < bitmapSize; i += bitsPerPage) {
    	ErrorCode error = m_storage.read(reinterpret_cast<char*>(&v[i/blockSize]), i);
    	if ( error != ErrorCode::E_NO_ERROR ) {
			return error;
		}
    }

    m_allocationTable.resize(bitmapSize);
    from_block_range(v.begin(), v.end(), m_allocationTable);

    // Add the unallocated pages to the free list
    for (uint64_t i = 0; i < m_allocationTable.size(); ++i) {
    	if (!m_allocationTable.test(i) && !isProtected(i)) {
    		m_freePages.push_back(i);
    	}
    }

    return ErrorCode::E_NO_ERROR;
}

ErrorCode BufferPool::storeAllocationTable() noexcept {
    std::vector<boost::dynamic_bitset<>::block_type> v(m_allocationTable.num_blocks());
    to_block_range(m_allocationTable, v.begin());

    uint64_t bitsPerPage = 8*m_storage.getPageSize(); 
    uint64_t blockSize = boost::dynamic_bitset<>::bits_per_block;

    for (uint64_t i = 0; i < m_allocationTable.size(); i += bitsPerPage) {
    	ErrorCode error = m_storage.write(reinterpret_cast<char*>(&v[i/blockSize]), i);
    	if ( error != ErrorCode::E_NO_ERROR ) {
			return error;
		}
    }

    return ErrorCode::E_NO_ERROR;
}

bool BufferPool::isProtected( const pageId_t& pId ) noexcept {
	bool retval = false;

	uint64_t bitsPerPage = 8*m_storage.getPageSize();
	if (pId%bitsPerPage == 0) {
		retval = true;
	}

	return retval;
}

ErrorCode BufferPool::flushDirtyBuffers() noexcept {
	// Look for dirty Buffer Pool slots and flush them to disk.
	for (int bId = 0; bId < m_descriptors.size(); ++bId) {
		if (m_descriptors[bId].m_inUse && m_descriptors[bId].m_dirty) {
			ErrorCode error = m_storage.write(getBuffer(bId), m_descriptors[bId].m_pageId);
			if ( error != ErrorCode::E_NO_ERROR ) {
				return error;
			}

			m_descriptors[bId].m_dirty = 0;
		}
	}

	return ErrorCode::E_NO_ERROR;
}

SMILE_NS_END

