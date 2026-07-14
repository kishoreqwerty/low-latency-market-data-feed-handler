add_executable(mdfh_hello_test test_hello.cpp)
target_link_libraries(mdfh_hello_test PRIVATE mdfh_project_options Catch2::Catch2WithMain)

include(Catch)
catch_discover_tests(mdfh_hello_test)
