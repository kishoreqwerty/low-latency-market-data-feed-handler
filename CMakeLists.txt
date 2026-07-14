cmake_minimum_required(VERSION 3.20)
project(market_data_feed_handler LANGUAGES CXX)

# --- Language standard ---------------------------------------------------
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

# --- Sanitizer selection for the `debug` config ---------------------------
# ASan and TSan cannot be linked into the same binary, so the debug config
# exposes a switch rather than enabling both at once. Build guide Phase 5
# requires a real TSan run on the SPSC ring buffer specifically; everything
# else defaults to ASan+UBSan. See cmake/Sanitizers.cmake.
set(MDFH_SANITIZER "address" CACHE STRING "Sanitizer for debug builds: address|thread|none")
set_property(CACHE MDFH_SANITIZER PROPERTY STRINGS address thread none)

include(cmake/Sanitizers.cmake)

add_library(mdfh_project_options INTERFACE)
target_compile_options(mdfh_project_options INTERFACE -Wall -Wextra -Wpedantic)
target_compile_features(mdfh_project_options INTERFACE cxx_std_23)

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
  mdfh_apply_sanitizers(mdfh_project_options "${MDFH_SANITIZER}")
else()
  target_compile_options(mdfh_project_options INTERFACE -O3 -DNDEBUG)
endif()

# --- Test dependencies (Catch2 + GoogleMock) ------------------------------
include(FetchContent)

FetchContent_Declare(
  Catch2
  GIT_REPOSITORY https://github.com/catchorg/Catch2.git
  GIT_TAG        v3.7.1
  GIT_SHALLOW    TRUE
)

set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
FetchContent_Declare(
  googletest
  GIT_REPOSITORY https://github.com/google/googletest.git
  GIT_TAG        v1.15.2
  GIT_SHALLOW    TRUE
)

FetchContent_MakeAvailable(Catch2 googletest)

list(APPEND CMAKE_MODULE_PATH "${catch2_SOURCE_DIR}/extras")

include(CTest)
enable_testing()

add_subdirectory(protocol)
add_subdirectory(orderbook)
add_subdirectory(concurrency)
add_subdirectory(io)
add_subdirectory(affinity)
add_subdirectory(publisher)
add_subdirectory(bench)
add_subdirectory(tests)
