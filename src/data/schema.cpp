

#include "schema.h"
#include "../memory/buffer_pool.h"
#include <cstring>

#include <iostream>

SMILE_NS_BEGIN


Schema::Schema(BufferPool* bufferPool) noexcept : 
  m_nextTypeId{0},
  p_bufferPool{bufferPool} {

}

ErrorCode Schema::loadSchema() noexcept {

  BufferPoolStatistics stats;
  p_bufferPool->getStatistics(&stats);
  uint32_t sizeForElements = stats.m_pageSize - sizeof(SchemaPageHeader);
  uint32_t numElementsPerPage = stats.m_pageSize / sizeof(SchemaElement) + (sizeForElements % sizeof(SchemaElement) != 0);

  BufferHandler handler;
  pageId_t nextPage = 1; 

  do {
    m_schemaPages.push_back(nextPage);
    p_bufferPool->pin(nextPage, &handler);
    SchemaPageHeader header     =  *reinterpret_cast<SchemaPageHeader*>(handler.m_buffer);
    SchemaElement*   elements   =   reinterpret_cast<SchemaElement*>(handler.m_buffer+sizeof(SchemaPageHeader));
    if(header.m_numElements > numElementsPerPage) {
      return ErrorCode::E_SCHEMA_PAGE_CORRUPTED;
    }
    for (int i = 0; i < header.m_numElements; ++i) {
      SchemaElement* nextElement = &elements[i];
      switch(nextElement->m_elementType) {
      case ElementType::E_NODE:
        {
          const char* name = nextElement->m_nodeInfo.m_name;
          m_nodes.insert(std::make_pair(name, *nextElement));
          break;
        }
      case ElementType::E_EDGE:
        {
          const char* name = nextElement->m_edgeInfo.m_name;
          m_edges.insert(std::make_pair(name, *nextElement));
          break;
        }
      case ElementType::E_PROPERTY:
        {
          const char* name = nextElement->m_propertyInfo.m_name;
          m_properties.insert(std::make_pair(name, *nextElement));
          break;
        }
      default:
        {
          return ErrorCode::E_SCHEMA_PAGE_CORRUPTED;
        }
      };
    }
    p_bufferPool->unpin(nextPage);
    nextPage = header.m_nextPage;
  } while(nextPage != INVALID_PAGE_ID);

  return ErrorCode::E_NO_ERROR;
}

ErrorCode Schema::persistSchema() noexcept {

  uint32_t numElements = m_nodes.size() + m_edges.size() + m_properties.size();
  BufferPoolStatistics stats;
  p_bufferPool->getStatistics(&stats);
  uint32_t sizeForElements = stats.m_pageSize - sizeof(SchemaPageHeader);
  uint32_t numElementsPerPage = stats.m_pageSize / sizeof(SchemaElement) + (sizeForElements % sizeof(SchemaElement) != 0);
  uint32_t numRequiredPages = numElements / numElementsPerPage + (numElements % numElementsPerPage != 0);

  // Putting all elements into a single array not to duplicate code
  std::vector<SchemaElement> elements;
  for( auto it = m_nodes.begin(); 
       it != m_nodes.end(); 
       ++it) {
    elements.push_back(it->second);
  }

  for( auto it = m_edges.begin(); 
       it != m_edges.end(); 
       ++it) {
    elements.push_back(it->second);
  }

  for( auto it = m_properties.begin(); 
       it != m_properties.end(); 
       ++it) {
    elements.push_back(it->second);
  }
  

  pageId_t currentPage = m_schemaPages.front();
  m_schemaPages.pop_front();
  BufferHandler handler;
  p_bufferPool->pin(currentPage, &handler);
  SchemaPageHeader* currentHeader   = reinterpret_cast<SchemaPageHeader*>(handler.m_buffer);
  currentHeader->m_numElements = 0;
  SchemaElement*    currentElements = reinterpret_cast<SchemaElement*>(handler.m_buffer + sizeof(SchemaPageHeader));

  for( auto it = elements.begin(); 
       it != elements.end(); 
       ++it) {

    currentElements[currentHeader->m_numElements] = *it;
    currentHeader->m_numElements++;
    
    if(currentHeader->m_numElements >= numElementsPerPage) {
      // writing current page
      if(m_schemaPages.size() == 0) {
        BufferHandler handler;
        p_bufferPool->alloc(&handler);
        currentHeader->m_nextPage = handler.m_pId;
        m_schemaPages.push_front(handler.m_pId);
        p_bufferPool->unpin(handler.m_pId);
      } else {
        currentHeader->m_nextPage = m_schemaPages.front();
      }
      p_bufferPool->setPageDirty(currentPage);
      p_bufferPool->unpin(currentPage);

      // Loading next page
      currentPage = m_schemaPages.front();
      m_schemaPages.pop_front();
      p_bufferPool->pin(currentPage, &handler);
      currentHeader   = reinterpret_cast<SchemaPageHeader*>(handler.m_buffer);
      currentHeader->m_numElements = 0;
      currentHeader->m_nextPage = INVALID_PAGE_ID;
      currentElements = reinterpret_cast<SchemaElement*>(handler.m_buffer + sizeof(SchemaPageHeader));
    }
  }

  currentHeader->m_nextPage = INVALID_PAGE_ID;
  p_bufferPool->setPageDirty(currentPage);
  p_bufferPool->unpin(currentPage);

  return ErrorCode::E_NO_ERROR;

}

ErrorCode Schema::newNodeType(const char* name) noexcept {

  if( std::strlen(name) >= (MAX_NAME_LENGTH - 1)) {
    return ErrorCode::E_SCHEMA_ELEMENT_NAME_TOO_LONG;
  };

  if( m_nodes.find(name) != m_nodes.end()) {
    return ErrorCode::E_SCHEMA_NODE_TYPE_ALREADY_EXISTS;
  };

  SchemaElement element;
  element.m_elementType = ElementType::E_NODE;
  std::strcpy(element.m_nodeInfo.m_name, name);
  element.m_nodeInfo.m_typeId = m_nextTypeId;
  element.m_structType = DataStructureType::E_NO_STRUCT;
  element.m_entryPage = INVALID_PAGE_ID;
  m_nextTypeId++;
  m_nodes.insert(std::make_pair(name, element));
  return ErrorCode::E_NO_ERROR;
}

ErrorCode Schema::getNodeType(const char* name, NodeInfo* nodeInfo ) const noexcept {

  auto it = m_nodes.find(name);
  if( it == m_nodes.end()) {
    return ErrorCode::E_SCHEMA_NODE_TYPE_NOT_EXISTS;
  };

  *nodeInfo = it->second.m_nodeInfo;
  return ErrorCode::E_NO_ERROR;
}

SMILE_NS_END


