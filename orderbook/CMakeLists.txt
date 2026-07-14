add_library(mdfh_orderbook
    order_book.cpp
)
target_include_directories(mdfh_orderbook PUBLIC ${CMAKE_SOURCE_DIR})
target_link_libraries(mdfh_orderbook PUBLIC mdfh_project_options mdfh_protocol)

add_executable(mdfh_orderbook_test test_order_book.cpp)
target_link_libraries(mdfh_orderbook_test PRIVATE mdfh_orderbook Catch2::Catch2WithMain)

include(Catch)
catch_discover_tests(mdfh_orderbook_test)

# Phase 3: object_pool.hpp wired in to replace hot-path new/malloc.
