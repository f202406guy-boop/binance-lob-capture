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
  # treat simdjson headers as system headers so our warning flags apply to our code only
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
  src/ws_client.cpp)
target_link_libraries(binance_capture PRIVATE lob_core Boost::headers OpenSSL::SSL OpenSSL::Crypto Threads::Threads)
target_compile_options(binance_capture PRIVATE ${WARNINGS})

enable_testing()
add_executable(unit_tests tests/unit_tests.cpp)
target_link_libraries(unit_tests PRIVATE lob_core)
target_compile_options(unit_tests PRIVATE ${WARNINGS})
add_test(NAME unit_tests COMMAND unit_tests)

# benchmark behind the --busy-poll choice (see README), not part of the tests
add_executable(wait_bench bench/wait_bench.cpp)
target_include_directories(wait_bench PRIVATE src)
target_link_libraries(wait_bench PRIVATE Threads::Threads)
target_compile_options(wait_bench PRIVATE ${WARNINGS})

if(ENABLE_ASAN)
  foreach(t lob_core binance_capture unit_tests wait_bench)
    target_compile_options(${t} PRIVATE -fsanitize=address,undefined -fno-omit-frame-pointer)
    target_link_options(${t} PRIVATE -fsanitize=address,undefined)
  endforeach()
endif()

if(ENABLE_TSAN)
  foreach(t lob_core binance_capture unit_tests wait_bench)
    target_compile_options(${t} PRIVATE -fsanitize=thread)
    target_link_options(${t} PRIVATE -fsanitize=thread)
  endforeach()
endif()
