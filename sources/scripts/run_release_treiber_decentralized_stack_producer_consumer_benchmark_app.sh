echo "procNum: $1"
cd ../install-release/bin/ || exit
mpiexec -np "$1" ./rma_treiber_decentralized_stack_producer_consumer_benchmark_app