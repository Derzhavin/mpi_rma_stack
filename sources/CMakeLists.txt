cmake_minimum_required(VERSION 3.10)
project(sources)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_INSTALL_RPATH $ORIGIN $ORIGIN/../lib)

add_subdirectory(third_party/spdlog)

add_subdirectory(libs)


file(GLOB
        RMA_TREIBER_CENTRAL_STACK_SIMPLE_PUSH_POP_TASK_APP_SOURCES
        apps/main_rma_treiber_central_stack_simple_push_pop_task_app.cpp
        src/logging.cpp
)
add_executable(
        rma_treiber_central_stack_simple_push_pop_task_app
        ${RMA_TREIBER_CENTRAL_STACK_SIMPLE_PUSH_POP_TASK_APP_SOURCES}
)
target_link_libraries(
        rma_treiber_central_stack_simple_push_pop_task_app
        PRIVATE
        sub::rma_stack
        spdlog
)
target_include_directories(
        rma_treiber_central_stack_simple_push_pop_task_app
        PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
        spdlog
)
install(TARGETS rma_treiber_central_stack_simple_push_pop_task_app DESTINATION bin/)



file(GLOB
        RMA_TREIBER_DECENTRALIZED_STACK_SIMPLE_PUSH_POP_TASK_APP_SOURCES
        apps/main_rma_treiber_decentralized_stack_simple_push_pop_task_app.cpp
        src/logging.cpp
        )
add_executable(
        rma_treiber_decentralized_stack_simple_push_pop_task_app
        ${RMA_TREIBER_DECENTRALIZED_STACK_SIMPLE_PUSH_POP_TASK_APP_SOURCES}
)
target_link_libraries(
        rma_treiber_decentralized_stack_simple_push_pop_task_app
        PRIVATE
        sub::rma_stack
        spdlog
)
target_include_directories(
        rma_treiber_decentralized_stack_simple_push_pop_task_app
        PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
        spdlog
)
install(TARGETS rma_treiber_decentralized_stack_simple_push_pop_task_app DESTINATION bin/)



file(GLOB
        RMA_TREIBER_CENTRAL_STACK_PRODUCER_CONSUMER_BENCHMARK_APP_SOURCES
        apps/main_rma_treiber_central_stack_producer_consumer_benchmark_app.cpp
        src/logging.cpp
        )
add_executable(
        rma_treiber_central_stack_producer_consumer_benchmark_app
        ${RMA_TREIBER_CENTRAL_STACK_PRODUCER_CONSUMER_BENCHMARK_APP_SOURCES}
)
target_link_libraries(
        rma_treiber_central_stack_producer_consumer_benchmark_app
        PRIVATE
        sub::rma_stack
        spdlog
)
target_include_directories(
        rma_treiber_central_stack_producer_consumer_benchmark_app
        PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
        spdlog
)
install(TARGETS rma_treiber_central_stack_producer_consumer_benchmark_app DESTINATION bin/)



file(GLOB
        RMA_TREIBER_DECENTRALIZED_STACK_PRODUCER_CONSUMER_BENCHMARK_APP_SOURCES
        apps/main_rma_treiber_decentralized_stack_producer_consumer_benchmark_app.cpp
        src/logging.cpp
        )
add_executable(
        rma_treiber_decentralized_stack_producer_consumer_benchmark_app
        ${RMA_TREIBER_DECENTRALIZED_STACK_PRODUCER_CONSUMER_BENCHMARK_APP_SOURCES}
)
target_link_libraries(
        rma_treiber_decentralized_stack_producer_consumer_benchmark_app
        PRIVATE
        sub::rma_stack
        spdlog
)
target_include_directories(
        rma_treiber_decentralized_stack_producer_consumer_benchmark_app
        PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
        spdlog
)
install(TARGETS rma_treiber_decentralized_stack_producer_consumer_benchmark_app DESTINATION bin/)



file(GLOB
        RMA_INNER_STACK_SIMPLE_PUSH_POP_TASK_APP_SOURCES
        apps/main_rma_inner_stack_simple_push_pop_task_app.cpp
        src/stack_tasks.cpp
        src/logging.cpp
        )
add_executable(
        rma_inner_stack_simple_push_pop_task_app
        ${RMA_INNER_STACK_SIMPLE_PUSH_POP_TASK_APP_SOURCES}
)
target_link_libraries(
        rma_inner_stack_simple_push_pop_task_app
        PRIVATE
        sub::rma_stack
        spdlog
)
target_include_directories(
        rma_inner_stack_simple_push_pop_task_app
        PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
        spdlog
)
install(TARGETS rma_inner_stack_simple_push_pop_task_app DESTINATION bin/)


install(TARGETS spdlog DESTINATION lib/)
