CMAKE_MINIMUM_REQUIRED(VERSION 3.8)

# Import gtest 
find_package(GTest REQUIRED)
include_directories(${GTEST_INCLUDE_DIR})

set(TEST_FILES)
file(GLOB TEST_FILES ./*.cc)

foreach(test_file_source ${TEST_FILES}) 
  get_filename_component(test_file_name ${test_file_source} NAME)
  if(${test_file_name} MATCHES "_test.cc$")
    string(REPLACE ".cc" "" test_exec_name ${test_file_name})
    add_executable(${test_exec_name} ${test_file_name})
    target_link_libraries(${test_exec_name} crest ${GTEST_BOTH_LIBRARIES} gflags pthread)
  endif()
endforeach()