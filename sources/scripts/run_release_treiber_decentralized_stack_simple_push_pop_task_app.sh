echo "procNum: $1"
cd ../install-release/bin/ || exit

if [ ! -d "decentralized" ]
then
  mkdir "decentralized"
fi

cd "decentralized" || exit

if [ ! -d "simple_push_pop" ]
then
  mkdir "simple_push_pop"
fi

cd "simple_push_pop" || exit

if [ -d $1 ]
then
  echo "cannot run the mpiexec because the directory $1 already exists"
  exit 1
fi

mkdir $1
cd $1 || exit
mpiexec -np "$1" ../../../rma_treiber_decentralized_stack_simple_push_pop_task_app