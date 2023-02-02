cmake_minimum_required(VERSION 3.10)
get_filename_component(project_dir ${CMAKE_CURRENT_LIST_DIR} NAME)
project(${project_dir})

add_library(
        ${PROJECT_NAME}
        INTERFACE
        include/RmaTreiberStack.hpp .
)
target_link_libraries(
        ${PROJECT_NAME}
        INTERFACE
        stack_interface
)
target_include_directories(
        ${PROJECT_NAME}
        INTERFACE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)
