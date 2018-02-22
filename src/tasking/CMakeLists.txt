

add_definitions(${DEFAULT_DEFINES})
include_directories(${Boost_INCLUDE_DIR})

if (UNIX)
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -fexceptions")
    set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -fexceptions")
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fexceptions")
    set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -fexceptions")
endif (UNIX)

add_library(tasking STATIC
  task.h
  task_pool.h
  task_pool.cpp
  tasking.h
  tasking.cpp
)

#target_link_libraries(storage base)
