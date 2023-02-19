#!/bin/bash

export CC=/usr/local/bin/mpicc
export CXX=/usr/local/bin/mpic++
export CXXFLAGS="-g3 -march=native -Wall -Wextra -pedantic"
export CFLAGS="-g3 -march=native -Wall -Wextra -pedantic"

BUILD_DIR=cmake-build-debug
INSTALL_DIR=install-debug

cmake -G "Unix Makefiles" \
      -S . \
      -B ${BUILD_DIR} \
      -D CMAKE_BUILD_TYPE=Debug

cmake --build ${BUILD_DIR} -j6 --verbose
cmake --install ${BUILD_DIR} --prefix ${INSTALL_DIR}
