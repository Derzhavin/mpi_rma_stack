echo "procNum: $1"
cd ../install-release/bin/ || exit
mpiexec -np "$1" ./rma_treiber_decentralized_stack_simple_push_pop_task_app