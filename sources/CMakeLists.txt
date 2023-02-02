cmake_minimum_required(VERSION 3.10)
project(sources)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_INSTALL_RPATH $ORIGIN $ORIGIN/../lib)

add_subdirectory(libs)

file(GLOB
        RMA_TREIBER_STACK_APP
        apps/main_rma_treiber_stack_app.cpp
)

add_executable(
        rma_treiber_stack_app
        ${RMA_TREIBER_STACK_APP}
)
target_link_libraries(
        rma_treiber_stack_app
        PRIVATE
        rma_treiber_stack
)
install(TARGETS rma_treiber_stack_app DESTINATION bin/)
