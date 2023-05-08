echo "procNum: $1"
cd ../install-debug/bin/ || exit
mpiexec -np "$1" ./rma_treiber_central_stack_producer_consumer_benchmark_app