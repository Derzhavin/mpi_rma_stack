echo "procNum: $1"
cd ../install-release/bin/ || exit

if [ ! -d "centralized" ]
then
  mkdir "centralized"
fi

cd "centralized" || exit

if [ ! -d "only_push" ]
then
  mkdir "only_push"
fi

cd "only_push" || exit

if [ -d $1 ]
then
  echo "cannot run the mpiexec because the directory $1 already exists"
  exit 1
fi

mkdir $1
cd $1 || exit
mpiexec -np "$1" ../../../rma_treiber_central_stack_only_push_benchmark_app