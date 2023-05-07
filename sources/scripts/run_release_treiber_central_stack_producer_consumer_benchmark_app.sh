echo "procNum: $1"
cd ..
mpiexec -np $1 ./install-release/bin/rma_treiber_central_stack_producer_consumer_benchmark_app