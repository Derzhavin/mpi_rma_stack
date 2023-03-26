NODES_NUM=$1
cd ..
mpiexec -np ${NODES_NUM} ./install-debug/bin/memory_pool_app