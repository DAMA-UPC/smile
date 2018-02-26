

#include "file_storage.h"

SMILE_NS_BEGIN


FileStorage::FileStorage() noexcept : 
  m_flags( std::ios_base::in | std::ios_base::out | std::ios_base::binary  )
{
}

FileStorage::~FileStorage() noexcept {
  if(m_dataFile) {
    m_dataFile.close();
  }

  if(m_configFile) {
    m_configFile.close();
  }
}

ErrorCode FileStorage::open( const std::string& path ) noexcept {
  m_dataFile.open( path, m_flags );
  if(!m_dataFile){
    return ErrorCode::E_STORAGE_INVALID_PATH;
  }

  m_configFile.open( path+".config", m_flags );
  if(!m_configFile){
    return ErrorCode::E_STORAGE_INVALID_PATH;
  }

  // Read FileStorageConfig from the first page of m_configFile
  m_configFile.seekg(0,std::ios_base::beg);
  m_configFile.read(reinterpret_cast<char*>(&m_config), sizeof(m_config));
  
  m_pageFiller.resize(getPageSize(),'\0');
  m_dataFile.seekg(0,std::ios_base::end);
  m_size = bytesToPage(m_dataFile.tellg());
  
  return ErrorCode::E_NO_ERROR;
}

ErrorCode FileStorage::create( const std::string& path, const FileStorageConfig& config, const bool& overwrite ) noexcept {
  if(!overwrite && std::ifstream(path)) {
    return ErrorCode::E_STORAGE_PATH_ALREADY_EXISTS;
  }

  m_dataFile.open( path, m_flags | std::ios_base::trunc );
  if(!m_dataFile) {
    return ErrorCode::E_STORAGE_INVALID_PATH;
  }

  m_configFile.open( path+".config", m_flags | std::ios_base::trunc );
  if(!m_configFile) {
    return ErrorCode::E_STORAGE_INVALID_PATH;
  }

  m_config = config;
  m_pageFiller.resize(getPageSize(),'\0');
  
  // Reserve space in m_configFile
  m_configFile.seekp(0,std::ios_base::beg);
  m_configFile.write(m_pageFiller.data(), m_pageFiller.size());
  if(!m_configFile) {
    return ErrorCode::E_STORAGE_OUT_OF_BOUNDS_WRITE;
  }

  // Write FileStorageConfig in the first page of m_configFile
  m_configFile.seekp(0,std::ios_base::beg);
  m_configFile.write(reinterpret_cast<char*>(&m_config), sizeof(m_config));
  m_configFile.flush();
  if(!m_configFile) {
    return ErrorCode::E_STORAGE_OUT_OF_BOUNDS_WRITE;
  }
  return ErrorCode::E_NO_ERROR;
}

ErrorCode FileStorage::close() noexcept {
  if(m_dataFile) {
    m_dataFile.close();
  }
  else {
    return ErrorCode::E_STORAGE_NOT_OPEN;
  }

  if(m_configFile) {
    m_configFile.close();
  }
  else {
    return ErrorCode::E_STORAGE_NOT_OPEN;
  }

  return ErrorCode::E_NO_ERROR;
}

ErrorCode FileStorage::reserve( const uint32_t& numPages, pageId_t* pageId ) noexcept {
  m_dataFile.seekp(0,std::ios_base::end);
  if(!m_dataFile) {
    return ErrorCode::E_STORAGE_CRITICAL_ERROR;
  }
  *pageId = bytesToPage(m_dataFile.tellp());
  m_dataFile.seekp(pageToBytes((numPages-1)),std::ios_base::end);
  m_dataFile.write(m_pageFiller.data(), m_pageFiller.size());
  if(!m_dataFile) {
    return ErrorCode::E_STORAGE_OUT_OF_BOUNDS_WRITE;
  }
  m_size = bytesToPage(m_dataFile.tellp());
  return ErrorCode::E_NO_ERROR;
}

ErrorCode FileStorage::read( char* data, const pageId_t& pageId ) noexcept {
  if(pageId == 0 || pageId >= m_size) {
    return ErrorCode::E_STORAGE_OUT_OF_BOUNDS_PAGE;
  }
  m_dataFile.seekg(pageToBytes(pageId), std::ios_base::beg);
  if(!m_dataFile) {
    return ErrorCode::E_STORAGE_OUT_OF_BOUNDS_PAGE;
  }
  m_dataFile.read(data,getPageSize());
  if(!m_dataFile) {
    return ErrorCode::E_STORAGE_OUT_OF_BOUNDS_READ;
  }
  return ErrorCode::E_NO_ERROR;
}

ErrorCode FileStorage::write( const char* data, const pageId_t& pageId ) noexcept {
  if(pageId == 0 || pageId >= m_size) {
    return ErrorCode::E_STORAGE_OUT_OF_BOUNDS_PAGE;
  }
  m_dataFile.seekp(pageToBytes(pageId), std::ios_base::beg);
  if(!m_dataFile) {
    return ErrorCode::E_STORAGE_OUT_OF_BOUNDS_PAGE;
  }
  m_dataFile.write(data,getPageSize());
  if(!m_dataFile) {
    return ErrorCode::E_STORAGE_OUT_OF_BOUNDS_WRITE;
  }
  return ErrorCode::E_NO_ERROR;
}

uint64_t FileStorage::size() const noexcept {
  return m_size;
}

const FileStorageConfig& FileStorage::config() const noexcept {
  return m_config;
}


uint32_t FileStorage::getPageSize() const noexcept {
  return m_config.m_pageSizeKB*1024;
}

pageId_t FileStorage::bytesToPage( const uint64_t& bytes ) const noexcept {
  return bytes / getPageSize();
}

uint64_t FileStorage::pageToBytes( const pageId_t& pageId ) const noexcept {
  return pageId * getPageSize();
}

SMILE_NS_END

