
add_definitions(${DEFAULT_DEFINES})

if (UNIX)
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -fexceptions")
    set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -fexceptions")
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fexceptions")
    set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -fexceptions")
endif (UNIX)

add_library(memory STATIC
  types.h
  buffer_pool.h
  buffer_pool.cpp
)

target_link_libraries(memory storage base numa)
#target_link_libraries(memory storage base)

#add_subdirectory(tests)
