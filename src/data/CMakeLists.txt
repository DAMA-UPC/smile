#include_directories(${SPA_BENCH_INCLUDE_DIR})
#include_directories(${Boost_INCLUDE_DIR})

add_definitions(${DEFAULT_DEFINES})

if (UNIX)
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -fno-exceptions")
    set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -fno-exceptions")
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fno-exceptions")
    set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -fno-exceptions")
endif (UNIX)

add_library(data STATIC 
  schema.h
  schema.cpp
)

#target_link_libraries(data base ${Boost_LIBRARIES})

