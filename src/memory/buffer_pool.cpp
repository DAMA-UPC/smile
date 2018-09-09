

#include "buffer_pool.h"
#include "../tasking/tasking.h"
#include <assert.h>
#include <algorithm>

#include <iostream>

SMILE_NS_BEGIN

BufferPool::BufferPool() noexcept {	
}

ErrorCode BufferPool::open( const BufferPoolConfig& bpConfig, const std::string& path ) {
	try {
		// Before continuing we need to make sure that no operations are being performed
		m_partitions.resize(bpConfig.m_numberOfPartitions);
		std::vector<std::unique_lock<std::shared_timed_mutex>> partitionGuards;
		for (uint32_t i = 0; i < bpConfig.m_numberOfPartitions; ++i) {
			 m_partitions[i].m_lock = std::make_unique<std::shared_timed_mutex>();
			 partitionGuards.push_back( std::unique_lock<std::shared_timed_mutex>(*m_partitions[i].m_lock) );
		}

		if ( bpConfig.m_poolSizeKB % (m_storage.getPageSize()/1024) != 0 ) {
			throw ErrorCode::E_BUFPOOL_POOL_SIZE_NOT_MULTIPLE_OF_PAGE_SIZE;
		}

		if ( getNumThreads() == 0 && bpConfig.m_prefetchingDegree > 0 ) {
			throw ErrorCode::E_BUFPOOL_NO_THREADS_AVAILABLE_FOR_PREFETCHING;
		}

		m_storage.open(path);
		m_config = bpConfig;
		uint32_t pageSizeKB = m_storage.getPageSize() / 1024;
		uint32_t poolElems = m_config.m_poolSizeKB / pageSizeKB;
		m_descriptors.resize(poolElems);
		for (int i = 0; i < poolElems; ++i) {
			m_descriptors[i].m_contentLock = std::make_unique<std::shared_timed_mutex>();
			m_descriptors[i].p_buffer = (char*) malloc( pageSizeKB*1024 );
			uint8_t part = i % m_config.m_numberOfPartitions;
			m_partitions[part].m_freeBuffers.push(i);
		}
		m_nextCSVictim = 0;
		m_currentThread = 0;

		loadAllocationTable();

		return ErrorCode::E_NO_ERROR;
	} catch (ErrorCode& error) {
		return error;
	} 
}

ErrorCode BufferPool::create( const BufferPoolConfig& bpConfig, const std::string& path, const FileStorageConfig& fsConfig, const bool& overwrite ) {
	try {
		// Before continuing we need to make sure that no operations are being performed
		m_partitions.resize(bpConfig.m_numberOfPartitions);
		std::vector<std::unique_lock<std::shared_timed_mutex>> partitionGuards;
		for (uint32_t i = 0; i < bpConfig.m_numberOfPartitions; ++i) {
			 m_partitions[i].m_lock = std::make_unique<std::shared_timed_mutex>();
			 partitionGuards.push_back( std::unique_lock<std::shared_timed_mutex>(*m_partitions[i].m_lock) );
		}

		if ( bpConfig.m_poolSizeKB % fsConfig.m_pageSizeKB != 0 ) {
			throw ErrorCode::E_BUFPOOL_POOL_SIZE_NOT_MULTIPLE_OF_PAGE_SIZE;
		}

		if ( getNumThreads() == 0 && bpConfig.m_prefetchingDegree > 0 ) {
			throw ErrorCode::E_BUFPOOL_NO_THREADS_AVAILABLE_FOR_PREFETCHING;
		}

		m_storage.create(path, fsConfig, overwrite);
		m_config = bpConfig;
		uint32_t pageSizeKB = m_storage.getPageSize() / 1024;
		uint32_t poolElems = m_config.m_poolSizeKB / pageSizeKB;
		m_descriptors.resize(poolElems);
		for (int i = 0; i < poolElems; ++i) {
			m_descriptors[i].m_contentLock = std::make_unique<std::shared_timed_mutex>();
			m_descriptors[i].p_buffer = (char*) malloc( pageSizeKB*1024 );
			uint8_t part = i % m_config.m_numberOfPartitions;
			m_partitions[part].m_freeBuffers.push(i);
		}
		m_nextCSVictim = 0;
		m_currentThread = 0;

		return ErrorCode::E_NO_ERROR;
	} catch (ErrorCode& error) {
		return error;
	} 
}

ErrorCode BufferPool::close() noexcept {
	try {
		// Flush dirty buffers
		flushDirtyBuffers();

		// Save m_allocationTable state to disk.
		storeAllocationTable();

		for (int i = 0; i < m_descriptors.size(); ++i) {
			free(m_descriptors[i].p_buffer);
		}

		m_storage.close();

		m_descriptors.clear();
		m_partitions.clear();
		m_allocationTable.clear();

		return ErrorCode::E_NO_ERROR;
	} catch (ErrorCode& error) {
		return error;
	} 
}

ErrorCode BufferPool::alloc( BufferHandler* bufferHandler ) noexcept {
	try {
		// Before continuing we need to make sure that no operations are being performed
		std::vector<std::unique_lock<std::shared_timed_mutex>> partitionGuards;
		for (uint32_t i = 0; i < m_config.m_numberOfPartitions; ++i) {
			 partitionGuards.push_back( std::unique_lock<std::shared_timed_mutex>(*m_partitions[i].m_lock) );
		}

		bufferId_t bId;
		pageId_t pId;
		// Check if there is a free page in any partition, else reserve space.
		bool found = false;
		for (uint8_t p = 0; p < m_config.m_numberOfPartitions && !found; ++p) {
			if ( !m_partitions[p].m_freePages.empty() ) {
				found = true;
				pId = m_partitions[p].m_freePages.front();
				m_partitions[p].m_freePages.pop_front();
			}
		}
		if (!found) {
			reservePages(1, &pId);
		}
		// Set page as allocated.		
		m_allocationTable.set(pId);
		partitionGuards.clear();

		// Take the lock of the partition
		uint8_t part = pId % m_config.m_numberOfPartitions;
		std::unique_lock<std::shared_timed_mutex> partitionGuard(*m_partitions[part].m_lock);		
		// Get an empty buffer pool slot for the page and update the buffer table.
		getEmptySlot(&bId, part);
		m_partitions[part].m_bufferToPageMap[pId] = bId;
		partitionGuard.unlock();

		std::unique_lock<std::shared_timed_mutex> contentGuard(*m_descriptors[bId].m_contentLock);
		// Fill the buffer descriptor.
		m_descriptors[bId].m_referenceCount = 1;
		m_descriptors[bId].m_usageCount = 1;
		m_descriptors[bId].m_dirty = 0;
		m_descriptors[bId].m_pageId = pId;

		// Set BufferHandler for the allocated buffer.
		bufferHandler->m_buffer 	= m_descriptors[bId].p_buffer;
		bufferHandler->m_pId 		= pId;
		bufferHandler->m_bId 		= bId;

		return ErrorCode::E_NO_ERROR;
	} catch (ErrorCode& error) {
		return error;
	} 
}

ErrorCode BufferPool::release( const pageId_t& pId ) noexcept {
	try {
		assert(pId <= m_storage.size() && "Page not allocated");
		assert(!isProtected(pId) && "Unable to access protected page");

		// Take the lock of the partition
		uint8_t part = pId % m_config.m_numberOfPartitions;
		std::unique_lock<std::shared_timed_mutex> partitionGuard(*m_partitions[part].m_lock);

		// Evict the page in case it is in the Buffer Pool
		auto it = m_partitions[part].m_bufferToPageMap.find(pId);
		if (it != m_partitions[part].m_bufferToPageMap.end()) {
			bufferId_t bId = it->second;
			// Delete page entry from buffer table.
			m_partitions[part].m_bufferToPageMap.erase(pId);
			m_partitions[part].m_freeBuffers.push(bId);
			// Set page as unallocated.
			m_allocationTable.set(pId, 0);
			m_partitions[part].m_freePages.push_back(pId);
			partitionGuard.unlock();

			// Update buffer descriptor.
			std::unique_lock<std::shared_timed_mutex> contentGuard(*m_descriptors[bId].m_contentLock);
			// If the buffer is dirty we must store it to disk.
			if( m_descriptors[bId].m_dirty ) {
				m_storage.write(m_descriptors[bId].p_buffer, m_descriptors[bId].m_pageId);
			}
			m_descriptors[bId].m_inUse = false;
			m_descriptors[bId].m_referenceCount = 0;
			m_descriptors[bId].m_usageCount = 0;
			m_descriptors[bId].m_dirty = 0;
			m_descriptors[bId].m_pageId = 0;
		}
		else {
			// Set page as unallocated.
			m_allocationTable.set(pId, 0);
			m_partitions[part].m_freePages.push_back(pId);
			partitionGuard.unlock();
		}		

		return ErrorCode::E_NO_ERROR;
	} catch (ErrorCode& error) {
		return error;
	} 
}

ErrorCode BufferPool::pin( const pageId_t& pId, BufferHandler* bufferHandler, bool enablePrefetch ) noexcept {
	try {
		assert(pId <= m_storage.size() && "Page not allocated");
		assert(!isProtected(pId) && "Unable to access protected page");

		// Take the lock of the partition
		uint8_t part = pId % m_config.m_numberOfPartitions;
		std::unique_lock<std::shared_timed_mutex> partitionGuard(*m_partitions[part].m_lock);

		bufferId_t bId;
		// Look for the desired page in the Buffer Pool.
		auto it = m_partitions[part].m_bufferToPageMap.find(pId);

		// If it is not already there, get an empty slot for the page and load it
		// from disk. Else take the corresponding slot. Update Buffer Descriptor 
		// accordingly.
		if (it == m_partitions[part].m_bufferToPageMap.end()) {
			getEmptySlot(&bId, part);
			m_partitions[part].m_bufferToPageMap[pId] = bId;
			partitionGuard.unlock();

			std::unique_lock<std::shared_timed_mutex> contentGuard(*m_descriptors[bId].m_contentLock);
			m_storage.read(m_descriptors[bId].p_buffer, pId);
			if (enablePrefetch) m_descriptors[bId].m_referenceCount = 1;
			if (enablePrefetch) m_descriptors[bId].m_usageCount = 1;
			m_descriptors[bId].m_dirty = 0;
			m_descriptors[bId].m_pageId = pId;
		}
		else {
			partitionGuard.unlock();

			bId = it->second;
			std::unique_lock<std::shared_timed_mutex> contentGuard(*m_descriptors[bId].m_contentLock);
			if (enablePrefetch) ++m_descriptors[bId].m_referenceCount;
			if (enablePrefetch) ++m_descriptors[bId].m_usageCount;
			m_descriptors[bId].m_pageId = pId;
		}		

	    if(bufferHandler != nullptr) {
	      bufferHandler->m_buffer = m_descriptors[bId].p_buffer;
	      bufferHandler->m_pId 	= pId;
	      bufferHandler->m_bId 	= bId;
	    }

		if (enablePrefetch && m_config.m_prefetchingDegree > 0) {
			// Set BufferHandler for the pinned buffer.
			struct Params{
				BufferPool*	m_bp;
				pageId_t 	m_pId;
				int16_t   m_degree;
				int16_t   m_size;
			};

			Params* params = new Params{};
			params->m_bp = this;
			params->m_pId = pId;
			params->m_degree = m_config.m_prefetchingDegree;
			params->m_size = m_storage.size();
			Task prefetchPage {
				[] (void * args) {
					Params* params = reinterpret_cast<Params*>(args);
					for (uint32_t i = 0; i < params->m_degree; ++i) {
						if ( params->m_pId+i+1 < params->m_size ) {
							params->m_bp->pin(params->m_pId, nullptr, false);
						}
					}
					delete params;
				},
				params
			};
			executeTaskAsync(m_currentThread, prefetchPage, nullptr);
			m_currentThread = (m_currentThread + 1) % getNumThreads();
    	}
		
		return ErrorCode::E_NO_ERROR;
	} catch (ErrorCode& error) {
		return error;
	} 
}

ErrorCode BufferPool::unpin( const pageId_t& pId ) noexcept {
	try {
		assert(pId <= m_storage.size() && "Page not allocated");
		assert(!isProtected(pId) && "Unable to access protected page");

		// Take the lock of the partition
		uint8_t part = pId % m_config.m_numberOfPartitions;
		std::unique_lock<std::shared_timed_mutex> partitionGuard(*m_partitions[part].m_lock);

		// Decrement page's reference count.
		auto it = m_partitions[part].m_bufferToPageMap.find(pId);
		assert(it != m_partitions[part].m_bufferToPageMap.end() && "Page not present");
		bufferId_t bId = it->second;
		partitionGuard.unlock();
		std::unique_lock<std::shared_timed_mutex> contentGuard(*m_descriptors[bId].m_contentLock);
		--m_descriptors[bId].m_referenceCount;	

		return ErrorCode::E_NO_ERROR;
	} catch (ErrorCode& error) {
		return error;
	} 
}

ErrorCode BufferPool::checkpoint() noexcept {
	try {
		// Before continuing we need to make sure that no operations are being performed
		std::vector<std::unique_lock<std::shared_timed_mutex>> partitionGuards;
		for (uint32_t i = 0; i < m_config.m_numberOfPartitions; ++i) {
			 partitionGuards.push_back( std::unique_lock<std::shared_timed_mutex>(*m_partitions[i].m_lock) );
		}

		// Flush dirty buffers
		flushDirtyBuffers();
		// Save m_allocationTable state to disk.
		storeAllocationTable();

		return ErrorCode::E_NO_ERROR;
	} catch (ErrorCode& error) {
		return error;
	} 
}

ErrorCode BufferPool::setPageDirty( const pageId_t& pId ) noexcept {
	try {
		// Take the lock of the partition
		uint8_t part = pId % m_config.m_numberOfPartitions;
		std::unique_lock<std::shared_timed_mutex> partitionGuard(*m_partitions[part].m_lock);

		auto it = m_partitions[part].m_bufferToPageMap.find(pId);
		assert(it != m_partitions[part].m_bufferToPageMap.end() && "Page not present");
		bufferId_t bId = it->second;
		partitionGuard.unlock();
		std::unique_lock<std::shared_timed_mutex> contentGuard(*m_descriptors[bId].m_contentLock);
		m_descriptors[bId].m_dirty = 1;

		return ErrorCode::E_NO_ERROR;
	} catch (ErrorCode& error) {
		return error;
	} 
}

ErrorCode BufferPool::getStatistics( BufferPoolStatistics* stats ) noexcept {
	try {
		// Before continuing we need to make sure that no operations are being performed
		std::vector<std::unique_lock<std::shared_timed_mutex>> partitionGuards;
		for (uint32_t i = 0; i < m_config.m_numberOfPartitions; ++i) {
			 partitionGuards.push_back( std::unique_lock<std::shared_timed_mutex>(*m_partitions[i].m_lock) );
		}

		uint64_t numAllocatedPages = 0;
		for (uint64_t i = 0; i < m_descriptors.size(); ++i) {
			std::shared_lock<std::shared_timed_mutex> contentGuard(*m_descriptors[i].m_contentLock);
			if (m_descriptors[i].m_inUse) {
				++numAllocatedPages;
			}
		}

		stats->m_numAllocatedPages = numAllocatedPages;
		stats->m_numReservedPages = m_storage.size();
	  	stats->m_pageSize = m_storage.getPageSize();

		return ErrorCode::E_NO_ERROR;
	} catch (ErrorCode& error) {
		return error;
	} 
}

ErrorCode BufferPool::checkConsistency() {
	try {
		// Before continuing we need to make sure that no operations are being performed
		std::vector<std::unique_lock<std::shared_timed_mutex>> partitionGuards;
		for (uint32_t i = 0; i < m_config.m_numberOfPartitions; ++i) {
			 partitionGuards.push_back( std::unique_lock<std::shared_timed_mutex>(*m_partitions[i].m_lock) );
		}

		for (uint64_t i = 0; i < m_allocationTable.size(); ++i) {
			// Take the lock of the partition
			uint8_t part = i % m_config.m_numberOfPartitions;
			std::unique_lock<std::shared_timed_mutex> partitionGuard(*m_partitions[part].m_lock);

			auto itFreePages = std::find(m_partitions[part].m_freePages.begin(), m_partitions[part].m_freePages.end(), i);
			auto itBuffer = m_partitions[part].m_bufferToPageMap.find(reinterpret_cast<pageId_t>(i));

			if (m_allocationTable.test(i)) {
				if (!isProtected(i) && itFreePages != m_partitions[part].m_freePages.end()) {
					throw ErrorCode::E_BUFPOOL_ALLOCATED_PAGE_IN_FREELIST;
				}
				else if (isProtected(i) && itFreePages != m_partitions[part].m_freePages.end()) {
					throw ErrorCode::E_BUFPOOL_PROTECTED_PAGE_IN_FREELIST;
				}
				
				if (itBuffer != m_partitions[part].m_bufferToPageMap.end()) {
					std::shared_lock<std::shared_timed_mutex> contentGuard(*m_descriptors[itBuffer->second].m_contentLock);
					if (!m_descriptors[itBuffer->second].m_inUse || !(m_descriptors[itBuffer->second].m_pageId == i)) {
						throw ErrorCode::E_BUFPOOL_BUFFER_DESCRIPTOR_INCORRECT_DATA;
					}
				}
			}
			else {
				if (!isProtected(i) && itFreePages == m_partitions[part].m_freePages.end()) {
					throw ErrorCode::E_BUFPOOL_FREE_PAGE_NOT_IN_FREELIST;
				}

				if (itBuffer != m_partitions[part].m_bufferToPageMap.end()) {
					throw ErrorCode::E_BUFPOOL_FREE_PAGE_MAPPED_TO_BUFFER;
				}
			}
		}

		return ErrorCode::E_NO_ERROR;
	} catch (ErrorCode& error) {
		return error;
	} 
}

ErrorCode BufferPool::getEmptySlot( bufferId_t* bId, uint8_t partition ) {
	bool found = false;

	// Look for an empty Buffer Pool slot.
	if (!m_partitions[partition].m_freeBuffers.empty()) {
		found = true;
		*bId = m_partitions[partition].m_freeBuffers.front();
		m_partitions[partition].m_freeBuffers.pop();
		std::unique_lock<std::shared_timed_mutex> contentGuard(*m_descriptors[*bId].m_contentLock);
		m_descriptors[*bId].m_inUse = true;
	}

	bool existUnpinnedPage = false;
	uint64_t start = m_nextCSVictim;

	// If there is no empty slot, use Clock Sweep algorithm to find a victim.
	while (!found) {
		// Check only buffers relative to our partition
		if ((m_nextCSVictim % m_config.m_numberOfPartitions) == partition) {
			// Check only unpinned pages
			std::unique_lock<std::shared_timed_mutex> contentGuard(*m_descriptors[m_nextCSVictim].m_contentLock);
			if (m_descriptors[m_nextCSVictim].m_referenceCount == 0)
			{
				existUnpinnedPage = true;

				if ( m_descriptors[m_nextCSVictim].m_usageCount == 0 ) {
					*bId = m_nextCSVictim;
					found = true;

					// If the buffer is dirty we must store it to disk.
					if( m_descriptors[*bId].m_dirty ) {
						m_storage.write(m_descriptors[*bId].p_buffer, m_descriptors[*bId].m_pageId);
					}

					// Delete page entry from buffer table.
					m_partitions[partition].m_bufferToPageMap.erase(m_descriptors[*bId].m_pageId);
				}
				else {
					--m_descriptors[m_nextCSVictim].m_usageCount;
				}	
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
		throw ErrorCode::E_BUFPOOL_OUT_OF_MEMORY;
	}

	return ErrorCode::E_NO_ERROR;
}

ErrorCode BufferPool::reservePages( const uint32_t& numPages, pageId_t* pId ) noexcept {
	// Reserve space in disk.
	m_storage.reserve(numPages, pId);

	// Add reserved pages to free list and increment the Allocation Table size to fit the new pages.
	for (uint64_t i = 0; i < numPages; ++i) {
		uint64_t page = (*pId)+i;
		uint8_t part = page % m_config.m_numberOfPartitions;
		if (!isProtected(page)) {
			m_partitions[part].m_freePages.push_back(page);	
		} 
		m_allocationTable.push_back(0);
	}

	// If the first reserved page is protected, an extra page must be reserved.
	if (isProtected(*pId)) {
		pageId_t pIdToReturn = (*pId) + 1;

		m_storage.reserve(1, pId);
		uint8_t part = *pId % m_config.m_numberOfPartitions;
		m_partitions[part].m_freePages.push_back(*pId);
		m_allocationTable.push_back(0);

		*pId = pIdToReturn;
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
    	m_storage.read(reinterpret_cast<char*>(&v[i/blockSize]), i);
    }

    m_allocationTable.resize(bitmapSize);
    from_block_range(v.begin(), v.end(), m_allocationTable);

    // If we do not resize to storage size we can potentially keep unallocated pages status.
    m_allocationTable.resize(m_storage.size());

    // Add the unallocated pages to the free list
    for (uint64_t i = 0; i < m_allocationTable.size(); ++i) {
    	if (!m_allocationTable.test(i) && !isProtected(i)) {
    		uint8_t part = i % m_config.m_numberOfPartitions;
    		m_partitions[i].m_freePages.push_back(i);
    	}
    }

    return ErrorCode::E_NO_ERROR;
}

ErrorCode BufferPool::storeAllocationTable() {
	uint64_t bitsPerPage = 8*m_storage.getPageSize(); 
	uint64_t blockSize = boost::dynamic_bitset<>::bits_per_block;
	uint64_t numPages = m_allocationTable.size()/bitsPerPage + (m_allocationTable.size()%bitsPerPage != 0);
	uint64_t vectorSize = bitsPerPage*numPages/blockSize;

    std::vector<boost::dynamic_bitset<>::block_type> v(vectorSize);
    to_block_range(m_allocationTable, v.begin());
    
    for (uint64_t i = 0; i < m_allocationTable.size(); i += bitsPerPage) {
    	m_storage.write(reinterpret_cast<char*>(&v[i/blockSize]), i);
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
		std::unique_lock<std::shared_timed_mutex> contentGuard(*m_descriptors[bId].m_contentLock);
		if (m_descriptors[bId].m_inUse && m_descriptors[bId].m_dirty) {
			m_storage.write(m_descriptors[bId].p_buffer, m_descriptors[bId].m_pageId);
			m_descriptors[bId].m_dirty = 0;
		}
	}

	return ErrorCode::E_NO_ERROR;
}

SMILE_NS_END

