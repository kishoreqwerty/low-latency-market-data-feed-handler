# Phase 8: CPU affinity / thread pinning helpers.
#
# macOS (thread_policy_set/THREAD_AFFINITY_POLICY, advisory hint) and Linux
# (pthread_setaffinity_np, hard pin) make genuinely different OS-level
# guarantees -- see thread_affinity.hpp -- so they're two separate .cpp
# files behind the same API, selected here by platform rather than by an
# #ifdef inside one file.
if(APPLE)
    set(MDFH_AFFINITY_SOURCE thread_affinity_macos.cpp)
elseif(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    set(MDFH_AFFINITY_SOURCE thread_affinity_linux.cpp)
else()
    message(FATAL_ERROR "mdfh_affinity: no thread_affinity implementation for this platform (${CMAKE_SYSTEM_NAME})")
endif()

add_library(mdfh_affinity
    ${MDFH_AFFINITY_SOURCE}
)
target_include_directories(mdfh_affinity PUBLIC ${CMAKE_SOURCE_DIR})
target_link_libraries(mdfh_affinity PUBLIC mdfh_project_options)

add_executable(mdfh_affinity_test
    test_thread_affinity.cpp
)
target_link_libraries(mdfh_affinity_test PRIVATE mdfh_affinity Catch2::Catch2WithMain)

include(Catch)
catch_discover_tests(mdfh_affinity_test)
