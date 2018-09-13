cmake_minimum_required(VERSION 2.6 FATAL_ERROR)
project(SMILE)
set( CMAKE_EXPORT_COMPILE_COMMANDS 1 )

#
# SPARKSEE version (major.minor.release)
#
set(VERSION_MAJOR "0")
set(VERSION_MINOR "0")
set(VERSION_RELEASE "1")

#
# RELEASE is the default build type
#
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE RELEASE)
endif(NOT CMAKE_BUILD_TYPE)

#
# Check 64bits architecture
#
if(CMAKE_SIZEOF_VOID_P MATCHES "4")
    set(ARCHTYPE "32")
else(CMAKE_SIZEOF_VOID_P MATCHES "4")
    set(ARCHTYPE "64") 
endif(CMAKE_SIZEOF_VOID_P MATCHES "4")

#
# Set default definitions
#
set(DEFAULT_DEFINES
)


#
# Set default platform definitions
#
if(UNIX)
  set(LINUX 1)
  set(SO "linux")
else(UNIX)
  set(SO "OTHER")
endif(UNIX)

#
# Set project paths
#
#set(SMILE_DIR "${SMILE_SOURCE_DIR}/src")
#set(SMILE_INCLUDE_DIR "${SMILE_DIR}/")

MESSAGE(STATUS ${SMILE_INCLUDE_DIR})

#
# Enabling testing
#
add_subdirectory(libs/gtest)
set(GTEST_INCLUDE_DIRS "${CMAKE_SOURCE_DIR}/libs/gtest/include" )
enable_testing()
set(GTEST_INCLUDE_DIRS  "${CMAKE_SOURCE_DIR}/tools/gtest/include/")
set(GTEST_LIBRARIES "gtest")

#
# Boost
#
set(Boost_USE_STATIC_LIBS OFF) 
set(Boost_USE_MULTITHREADED ON)  
set(Boost_USE_STATIC_RUNTIME OFF) 
FIND_PACKAGE(Boost 1.66 COMPONENTS program_options date_time thread system locale context REQUIRED)

#
# Numa
#
set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake/modules/;${CMAKE_MODULE_PATH}")
FIND_PACKAGE(NUMA REQUIRED)

#
# Set output folders
#
set(SMILE_LIB_OUTPUT_DIR "${CMAKE_BINARY_DIR}/lib")
set(SMILE_BIN_OUTPUT_DIR "${CMAKE_BINARY_DIR}/bin")
set(SMILE_TEST_OUTPUT_DIR "${SMILE_BIN_OUTPUT_DIR}/test")
set(SMILE_REGTEST_OUTPUT_DIR "${SMILE_BIN_OUTPUT_DIR}/regtest")


#
# Summary messages
#
message(STATUS "SMILE v" ${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_RELEASE})
message(STATUS "PLATFORM ${SO} ${ARCHTYPE}bits")
message(STATUS "BUILD_TYPE ${CMAKE_BUILD_TYPE}")
message(STATUS "DEFAULT DEFINES: " ${DEFAULT_DEFINES})
message(STATUS "CMAKE_SYSTEM_NAME: " ${CMAKE_SYSTEM_NAME})
message(STATUS "CMAKE_SYSTEM_PROCESSOR: " ${CMAKE_SYSTEM_PROCESSOR})

add_subdirectory(src)
