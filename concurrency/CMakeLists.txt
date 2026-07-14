# Phase 4: per-shard sequence gap detector.
add_library(mdfh_concurrency INTERFACE)
target_include_directories(mdfh_concurrency INTERFACE ${CMAKE_SOURCE_DIR})
target_link_libraries(mdfh_concurrency INTERFACE mdfh_project_options mdfh_protocol)

add_executable(mdfh_concurrency_test test_gap_detector.cpp)
target_link_libraries(mdfh_concurrency_test PRIVATE mdfh_concurrency Catch2::Catch2WithMain)

include(Catch)
catch_discover_tests(mdfh_concurrency_test)

# Phase 5: lock-free SPSC ring buffer with drop-oldest backpressure.
