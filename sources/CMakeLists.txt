cmake_minimum_required(VERSION 3.10)
project(sources)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_INSTALL_RPATH $ORIGIN $ORIGIN/../lib)

add_subdirectory(libs)

file(GLOB
        RMA_TREIBER_STACK_SIMPLE_PUSH_POP_TASK_APP_SOURCES
        apps/main_rma_treiber_stack_simple_push_pop_task_app.cpp
        src/logging.cpp
)
add_executable(
        rma_treiber_stack_simple_push_pop_task_app
        ${RMA_TREIBER_STACK_SIMPLE_PUSH_POP_TASK_APP_SOURCES}
)
target_link_libraries(
        rma_treiber_stack_simple_push_pop_task_app
        PRIVATE
        sub::rma_stack
)
target_include_directories(
        rma_treiber_stack_simple_push_pop_task_app
        PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
)
install(TARGETS rma_treiber_stack_simple_push_pop_task_app DESTINATION bin/)


file(GLOB
        MEMORY_POOL_APP_SOURCES
        apps/main_centralized_memory_pool_app.cpp
        src/centralized_memory_pool_tasks.cpp
        src/logging.cpp
        )
add_executable(
        memory_pool_app
        ${MEMORY_POOL_APP_SOURCES}
)
target_link_libraries(
        memory_pool_app
        PRIVATE
        sub::memory_pool
)
target_include_directories(
        memory_pool_app
        PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
)
install(TARGETS memory_pool_app DESTINATION bin/)