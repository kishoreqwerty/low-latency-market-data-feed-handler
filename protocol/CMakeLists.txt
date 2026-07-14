add_library(mdfh_protocol
    encoder.cpp
    decoder.cpp
)
target_include_directories(mdfh_protocol PUBLIC ${CMAKE_SOURCE_DIR})
target_link_libraries(mdfh_protocol PUBLIC mdfh_project_options)

add_executable(mdfh_protocol_test test_protocol.cpp)
target_link_libraries(mdfh_protocol_test PRIVATE mdfh_protocol Catch2::Catch2WithMain)

include(Catch)
catch_discover_tests(mdfh_protocol_test)
