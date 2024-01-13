cmake_minimum_required(VERSION 3.26)
project(1brc LANGUAGES CXX)

include(FetchContent)
FetchContent_Declare(
  marl
  GIT_REPOSITORY https://github.com/google/marl.git
  GIT_TAG dbf097e43824d4b4ba45d5b696d86e147a4b9b00
)
FetchContent_MakeAvailable(marl)

add_executable(${PROJECT_NAME})
target_sources(${PROJECT_NAME} PRIVATE main.cpp)
target_link_libraries(${PROJECT_NAME} PRIVATE marl)
target_compile_features(${PROJECT_NAME} PRIVATE cxx_std_20)
