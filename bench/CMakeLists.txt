add_executable(mdfh_allocation_bench allocation_bench.cpp)
target_link_libraries(mdfh_allocation_bench PRIVATE mdfh_orderbook mdfh_alloc_counter mdfh_project_options)

# Phase 10: latency_bench.cpp (tick-to-book-update percentiles).
