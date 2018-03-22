

#ifndef _SMILE_DATA_SCHEMA_H_
#define _SMILE_DATA_SCHEMA_H_ value

#include "../base/error.h"
#include "../base/platform.h"
#include "../storage/types.h"
#include <map>
#include <list>

SMILE_NS_BEGIN

class BufferPool;

#define MAX_NAME_LENGTH 64

using typeId_t = uint16_t;

enum class ElementType : uint8_t {
  E_NODE,
  E_EDGE,
  E_PROPERTY
};

/**
 * @brief Struct to store information about a node type
 */
struct NodeInfo {
  char        m_name[MAX_NAME_LENGTH];
  uint16_t    m_typeId;
};

/**
 * @brief Struct to store information about an edge type
 */
struct EdgeInfo {
  char        m_name[MAX_NAME_LENGTH];
  uint16_t    m_typeId;
};

/**
 * @brief  The data type of a property
 */
enum class PropertyType : uint16_t {
  E_INT,
  E_LONG,
  E_FLOAT,
  E_DOUBLE,
  E_STRING
};

/**
 * @brief Struct to store information about a property
 */
struct PropertyInfo {
  char            m_name[MAX_NAME_LENGTH];
  PropertyType    m_type;
};

/**
 * @brief Enum class to enumerate the different low level structures
 */
enum class DataStructureType : uint16_t {
  E_TABLE,
  E_INDEX,
  E_CSR,
  E_NO_STRUCT // Nodes Types require no special data, just the type id
};

struct SchemaElement {

  /**
   * @brief The type of schema element
   */
  ElementType     m_elementType;

  /**
   * @brief The specific information depending on the type of element
   */
  union {
    NodeInfo      m_nodeInfo;
    EdgeInfo      m_edgeInfo;
    PropertyInfo  m_propertyInfo;
  };

  /**
   * @brief The structure type of this schema item
   */
  DataStructureType m_structType;

  /**
   * @brief The page that is the entry point of this structure
   */
  pageId_t   m_entryPage;

};

struct SchemaPageHeader {
  uint32_t m_numElements;
  pageId_t m_nextPage;
};

class Schema final {
  SMILE_NOT_COPYABLE(Schema);

public:
  Schema(BufferPool* bufferPool) noexcept;
  ~Schema() = default;

  /**
   * @brief Loads the schema from the storage 
   */
  ErrorCode loadSchema() noexcept;

  /**
   * @brief Persists the schema to the storage 
   */
  ErrorCode persistSchema() noexcept;


  /**
   * @brief Creates a new node type
   *
   * @param name The name of the node type
   *
   * @return An error code indicating whether there was an error or not
   */
  ErrorCode newNodeType(const char* name) noexcept ;

  /**
   * @brief Gets the type information of a node 
   *
   * @param name The name of the node type
   * @param nodeInfo The information about the requested node  
   *
   * @return An error code indicating whether there was an error or not
   */
  ErrorCode getNodeType(const char* name, NodeInfo* nodeInfo ) const noexcept;

private:

  /**
   * @brief The next type Id
   */
  typeId_t        m_nextTypeId;

  /**
   * @brief Pointer to the buffer pool to retrieve from and write the data to
   */
  BufferPool* p_bufferPool;

  /**
   * @brief The list of schema pages already allocated to store the schema
   */
  std::list<pageId_t> m_schemaPages;

  /**
   * @brief The Node schema elements
   */
  std::map<std::string, SchemaElement>  m_nodes;

  /**
   * @brief The Edge schema elements
   */
  std::map<std::string, SchemaElement>  m_edges;

  /**
   * @brief The property schema elements
   */
  std::map<std::string, SchemaElement>  m_properties;


};


SMILE_NS_END

#endif /* ifndef _SMILE_DATA_SCHEMA_H_ */
