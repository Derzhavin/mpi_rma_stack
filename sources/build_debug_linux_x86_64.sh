#!/bin/bash

export CC=/usr/bin/mpicc
export CXX=/usr/bin/mpic++
export CXXFLAGS="-g3 -march=native -Wall -Wextra -pedantic"
export CFLAGS="-g3 -march=native -Wall -Wextra -pedantic"

cmake -G "Unix Makefiles" \
      -S . \
      -B cmake-build-debug \
      -D CMAKE_BUILD_TYPE=Debug

cmake --build cmake-build-debug -j6 --verbose
cmake --install  cmake-build-debug --prefix install-debug-linux-x86_64
