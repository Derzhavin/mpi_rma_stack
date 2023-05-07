echo "procNum: $1"
cd ..
mpiexec -np $1 ./install-debug/bin/rma_treiber_stack_simple_push_pop_task_app