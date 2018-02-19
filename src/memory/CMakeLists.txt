
add_definitions(${DEFAULT_DEFINES})

add_library(memory STATIC
  types.h
  buffer_pool.h
  buffer_pool.cpp
)

#target_link_libraries(memory storage base)
#target_link_libraries(memory storage base)

#add_subdirectory(tests)
