cmake_minimum_required(VERSION 3.10)
get_filename_component(project_dir ${CMAKE_CURRENT_LIST_DIR} NAME)
project(${project_dir})

find_package(MPI REQUIRED)

add_library(
        ${PROJECT_NAME}
        INTERFACE
        include/RmaTreiberStack.h
        ${CMAKE_CURRENT_SOURCE_DIR}
)
add_library(sub::${PROJECT_NAME} ALIAS ${PROJECT_NAME})

target_link_libraries(
        ${PROJECT_NAME}
        INTERFACE
        sub::stack_interface
        sub::custom_mpi_extensions
        ${MPI_C_LIBRARIES}
)
target_include_directories(
        ${PROJECT_NAME}
        INTERFACE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)
