echo "procNum: $1"
cd ../install-debug/bin/ || exit
mpiexec -np "$1" ./rma_treiber_decentralized_stack_only_push_benchmark_app