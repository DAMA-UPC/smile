include_directories(${GTEST_INCLUDE_DIRS})
include_directories(${SMILE_INCLUDE_DIR})

include_directories(${GTEST_INCLUDE_DIRS})

function(create_test NAME)
  add_executable(${NAME}
    ${NAME}.cpp
    )

  target_link_libraries(${NAME} ${GTEST_LIBRARIES} ${SMILE_LIBRARIES} ) 
  add_test(${NAME} ${SMILE_TEST_OUTPUT_DIR}/${NAME})

  set_target_properties( ${NAME} 
    PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${SMILE_TEST_OUTPUT_DIR}
    )
endfunction(create_test)

SET(TESTS "file_storage_test" "tasking_test")

foreach( TEST ${TESTS} )
  create_test(${TEST})
endforeach( TEST )

add_custom_target(tests COMMAND ${CMAKE_CTEST_COMMAND}
  DEPENDS ${TESTS})

