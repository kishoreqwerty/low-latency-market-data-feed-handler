add_library(mdfh_orderbook
    order_book.cpp
)
target_include_directories(mdfh_orderbook PUBLIC ${CMAKE_SOURCE_DIR})
target_link_libraries(mdfh_orderbook PUBLIC mdfh_project_options mdfh_protocol)

# Overrides global operator new/delete to count real heap traffic. Kept out
# of mdfh_orderbook itself -- only test/bench binaries that need to audit
# allocation behavior should link it.
add_library(mdfh_alloc_counter STATIC
    test_alloc_counter.cpp
)
target_include_directories(mdfh_alloc_counter PUBLIC ${CMAKE_SOURCE_DIR})
target_link_libraries(mdfh_alloc_counter PUBLIC mdfh_project_options)

add_executable(mdfh_orderbook_test
    test_order_book.cpp
    test_object_pool.cpp
)
target_link_libraries(mdfh_orderbook_test PRIVATE mdfh_orderbook mdfh_alloc_counter Catch2::Catch2WithMain)

include(Catch)
catch_discover_tests(mdfh_orderbook_test)

find_package(Threads REQUIRED)

# Real multi-threaded verification for the per-shard pool redesign -- kept
# separate from the fast unit tests above so it can be run/verified
# specifically under debug-tsan (same reasoning as mdfh_spsc_stress_test).
add_executable(mdfh_pool_concurrency_test test_object_pool_concurrency.cpp)
target_link_libraries(mdfh_pool_concurrency_test PRIVATE mdfh_orderbook Catch2::Catch2WithMain Threads::Threads)
catch_discover_tests(mdfh_pool_concurrency_test)
