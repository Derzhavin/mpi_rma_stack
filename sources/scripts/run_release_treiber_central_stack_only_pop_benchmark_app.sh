echo "procNum: $1"
cd ../install-release/bin/ || exit
mpiexec -np "$1" ./rma_treiber_central_stack_only_pop_benchmark_app