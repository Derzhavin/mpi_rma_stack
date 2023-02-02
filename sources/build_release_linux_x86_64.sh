#!/bin/bash

export CC=mpicc
export CXX=mpic++
export CXXFLAGS="-O3 -march=native -Wall -Wextra -pedantic"
export CFLAGS="-O3 -march=native -Wall -Wextra -pedantic"

cmake -G "Unix Makefiles" \
      -S . \
      -B cmake-build-release \
      -D CMAKE_BUILD_TYPE=Release

cmake --build cmake-build-release -j6 --verbose
cmake --install  cmake-build-release --prefix install-release-linux-x86_64
