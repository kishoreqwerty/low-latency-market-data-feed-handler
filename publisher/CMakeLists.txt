# Phase 9: per-shard publisher threads emitting book deltas downstream.
#
# Deliberately depends only on mdfh_concurrency + mdfh_protocol, NOT on
# mdfh_io -- see book_delta.hpp's header comment for why (io/ depends on
# publisher/ for the BookDelta/DeltaQueue types, not the other way
# around, so io/pipeline_runner.cpp can spawn publisher threads using this
# library without creating a cycle).
add_library(mdfh_publisher
    publisher.cpp
)
target_include_directories(mdfh_publisher PUBLIC ${CMAKE_SOURCE_DIR})
target_link_libraries(mdfh_publisher PUBLIC mdfh_project_options mdfh_concurrency mdfh_protocol)

add_executable(mdfh_publisher_test
    test_publisher.cpp
)
target_link_libraries(mdfh_publisher_test PRIVATE mdfh_publisher Catch2::Catch2WithMain)

include(Catch)
catch_discover_tests(mdfh_publisher_test)
