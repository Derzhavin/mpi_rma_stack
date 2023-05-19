#PBS -l select=$($1):ncpus=1:mpiprocs=1:mem=1000m,place=free

echo "procNum: $1"
cd ../install-debug/bin/ || exit

if [ ! -d "decentralized" ]
then
  mkdir "decentralized"
fi

cd "decentralized" || exit

if [ ! -d "random_op" ]
then
  mkdir "random_op"
fi

cd "random_op" || exit

if [ -d $1 ]
then
  echo "cannot run the mpiexec because the directory $1 already exists"
  exit 1
fi

mkdir $1
cd $1 || exit
mpiexec -np "$1" ../../../rma_treiber_decentralized_stack_random_operation_benchmark_app