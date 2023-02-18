cmake_minimum_required(VERSION 3.10)
project(sources)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_INSTALL_RPATH $ORIGIN $ORIGIN/../lib)

add_subdirectory(libs)

file(GLOB
        RMA_TREIBER_STACK_SIMPLE_PUSH_POP_TASK_APP_SOURCES
        apps/main_rma_treiber_stack_simple_push_pop_task_app.cpp
        include/stack_tasks.h
)

add_executable(
        rma_treiber_stack_simple_push_pop_task_app
        ${RMA_TREIBER_STACK_SIMPLE_PUSH_POP_TASK_APP_SOURCES}
)
target_link_libraries(
        rma_treiber_stack_simple_push_pop_task_app
        PRIVATE
        rma_stack
)
target_include_directories(
        rma_treiber_stack_simple_push_pop_task_app
        PRIVATE
        .
)
install(TARGETS rma_treiber_stack_simple_push_pop_task_app DESTINATION bin/)

file(GLOB
        RMA_CENTRAL_LIST_SIMPLE_PUSH_POP_TASK_APP_SOURCES
        apps/main_rma_central_list_simple_push_pop_task_app.cpp
        include/list_tasks.h
        )

add_executable(
        rma_central_list_simple_push_pop_task_app
        ${RMA_CENTRAL_LIST_SIMPLE_PUSH_POP_TASK_APP_SOURCES}
)
target_link_libraries(
        rma_central_list_simple_push_pop_task_app
        PRIVATE
        rma_list
)
target_include_directories(
        rma_central_list_simple_push_pop_task_app
        PRIVATE
        .
)
install(TARGETS rma_central_list_simple_push_pop_task_app DESTINATION bin/)