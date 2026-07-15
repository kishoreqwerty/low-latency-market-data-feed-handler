add_library(mdfh_io
    shard_demux.cpp
)
target_include_directories(mdfh_io PUBLIC ${CMAKE_SOURCE_DIR})
target_link_libraries(mdfh_io PUBLIC mdfh_project_options mdfh_protocol mdfh_orderbook mdfh_concurrency)

add_executable(mdfh_io_test test_shard_demux.cpp)
target_link_libraries(mdfh_io_test PRIVATE mdfh_io Catch2::Catch2WithMain)

include(Catch)
catch_discover_tests(mdfh_io_test)

# Phase 7: coroutine-based async network reader.
