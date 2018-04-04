

add_definitions(${DEFAULT_DEFINES})

if (UNIX)
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -fexceptions")
    set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -fexceptions")
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fexceptions")
    set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -fexceptions")
endif (UNIX)

add_library(storage STATIC
  file_storage.cpp
  file_storage.h
  sequential_storage.h
  types.h
)

#target_link_libraries(storage base)
