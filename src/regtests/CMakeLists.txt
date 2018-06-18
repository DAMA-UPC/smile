include_directories(${GTEST_INCLUDE_DIRS})
include_directories(${SMILE_INCLUDE_DIR})

include_directories(${GTEST_INCLUDE_DIRS})

function(create_regtest NAME)
  add_executable(${NAME}
    ${NAME}.cpp
    )

  target_link_libraries(${NAME} ${GTEST_LIBRARIES} ${SMILE_LIBRARIES} ) 
  add_test(${NAME} ${SMILE_REGTEST_OUTPUT_DIR}/${NAME})

  set_target_properties( ${NAME} 
    PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${SMILE_REGTEST_OUTPUT_DIR}
    )
endfunction(create_regtest)

SET(TESTS "alloc_regtest" "groupby_regtest" "hashjoin_regtest" "scan_regtest" "loadgraph_regtest" "bfsgraph_regtest")

foreach( TEST ${TESTS} )
  create_regtest(${TEST})
endforeach( TEST )

add_custom_target(regtests COMMAND ${CMAKE_CTEST_COMMAND}
  DEPENDS ${TESTS})