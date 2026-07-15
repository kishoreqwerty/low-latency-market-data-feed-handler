find_package(Threads REQUIRED)

add_library(mdfh_io
    shard_demux.cpp
    feed_generator.cpp
    async_network_reader.cpp
    pipeline_runner.cpp
)
target_include_directories(mdfh_io PUBLIC ${CMAKE_SOURCE_DIR})
target_link_libraries(mdfh_io PUBLIC mdfh_project_options mdfh_protocol mdfh_orderbook mdfh_concurrency Threads::Threads)

add_executable(mdfh_io_test
    test_shard_demux.cpp
    test_feed_generator.cpp
    test_async_network_reader.cpp
)
target_link_libraries(mdfh_io_test PRIVATE mdfh_io Catch2::Catch2WithMain)

include(Catch)
catch_discover_tests(mdfh_io_test)

# Phase 7: real multi-threaded end-to-end pipeline test, kept in its own
# executable (same reasoning as mdfh_spsc_stress_test in Phase 5): it spins
# up real OS threads and needs to be run/verified specifically under
# debug-tsan, separately from the fast single-threaded unit tests above.
add_executable(mdfh_pipeline_e2e_test test_pipeline_e2e.cpp)
target_link_libraries(mdfh_pipeline_e2e_test PRIVATE mdfh_io Catch2::Catch2WithMain)
catch_discover_tests(mdfh_pipeline_e2e_test)
