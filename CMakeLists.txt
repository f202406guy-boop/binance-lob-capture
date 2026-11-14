# This is the build file. The shared code (CSV, decimal parsing, the order book, the processor
# and replay) goes into one static library, lob_core, so binance_capture, the unit tests and
# the REST verifier all link the same code and nothing is compiled twice. The network part
# (ws_client, rest_client) and main.cpp only go into binance_capture, since that's the only
# target that needs Boost and OpenSSL.
#
# simdjson comes from the system if CMake finds v3, otherwise v3.10.1 is downloaded at
# configure time. In that case I mark its headers as system headers, so my warning flags only
# apply to my own code. I turned -Wsign-conversion off because mixing size_t indices with
# signed counts made it noisy, and -Wconversion still catches narrowing. If no build type is
# given it defaults to Release.
#
# unit_tests is put in build/tests and there's a "tests" target, so the assignment's commands
# (cmake --build build --target tests, then ./build/tests/unit_tests) work as written, and
# ctest runs it too. verify_against_rest (the full-depth check against saved REST snapshots)
# and wait_bench (the benchmark behind --busy-poll) are built too but aren't part of ctest.
# ENABLE_ASAN adds ASan + UBSan and ENABLE_TSAN adds ThreadSanitizer to all of our targets.

cmake_minimum_required(VERSION 3.16)
project(binance_lob_capture LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

option(ENABLE_ASAN "Build with AddressSanitizer + UBSan" OFF)
option(ENABLE_TSAN "Build with ThreadSanitizer" OFF)

include(FetchContent)

find_package(simdjson 3 QUIET)
if(NOT simdjson_FOUND)
  FetchContent_Declare(
    simdjson
    GIT_REPOSITORY https://github.com/simdjson/simdjson.git
    GIT_TAG v3.10.1
    GIT_SHALLOW TRUE)
  FetchContent_MakeAvailable(simdjson)
  get_target_property(SIMDJSON_INCLUDES simdjson INTERFACE_INCLUDE_DIRECTORIES)
  set_target_properties(simdjson PROPERTIES INTERFACE_SYSTEM_INCLUDE_DIRECTORIES "${SIMDJSON_INCLUDES}")
endif()

find_package(Boost 1.74 REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(Threads REQUIRED)

set(WARNINGS -Wall -Wextra -Wpedantic -Wshadow -Wconversion -Wno-sign-conversion)

add_library(lob_core STATIC
  src/csv.cpp
  src/order_book.cpp
  src/processor.cpp
  src/replay.cpp)
target_include_directories(lob_core PUBLIC src)
target_link_libraries(lob_core PUBLIC simdjson::simdjson)
target_compile_options(lob_core PRIVATE ${WARNINGS})

add_executable(binance_capture
  src/main.cpp
  src/rest_client.cpp
  src/ws_client.cpp)
target_link_libraries(binance_capture PRIVATE lob_core Boost::headers OpenSSL::SSL OpenSSL::Crypto Threads::Threads)
target_compile_options(binance_capture PRIVATE ${WARNINGS})

enable_testing()
add_executable(unit_tests tests/unit_tests.cpp)
target_link_libraries(unit_tests PRIVATE lob_core)
target_compile_options(unit_tests PRIVATE ${WARNINGS})
add_test(NAME unit_tests COMMAND unit_tests)
set_target_properties(unit_tests PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/tests)
add_custom_target(tests DEPENDS unit_tests)

add_executable(verify_against_rest tests/verify_against_rest.cpp)
target_link_libraries(verify_against_rest PRIVATE lob_core)
target_compile_options(verify_against_rest PRIVATE ${WARNINGS})

add_executable(wait_bench bench/wait_bench.cpp)
target_include_directories(wait_bench PRIVATE src)
target_link_libraries(wait_bench PRIVATE Threads::Threads)
target_compile_options(wait_bench PRIVATE ${WARNINGS})

if(ENABLE_ASAN)
  foreach(t lob_core binance_capture unit_tests verify_against_rest wait_bench)
    target_compile_options(${t} PRIVATE -fsanitize=address,undefined -fno-omit-frame-pointer)
    target_link_options(${t} PRIVATE -fsanitize=address,undefined)
  endforeach()
endif()

if(ENABLE_TSAN)
  foreach(t lob_core binance_capture unit_tests verify_against_rest wait_bench)
    target_compile_options(${t} PRIVATE -fsanitize=thread)
    target_link_options(${t} PRIVATE -fsanitize=thread)
  endforeach()
endif()
