#!/bin/bash

cd ..

export CC=/usr/local/bin/mpicc
export CXX=/usr/local/bin/mpic++
export CXXFLAGS="-O3 -march=native -Wall -Wextra -pedantic"
export CFLAGS="-O3 -march=native -Wall -Wextra -pedantic"

BUILD_DIR=cmake-build-release
INSTALL_DIR=install-release

conan install . --install-folder ${BUILD_DIR} -o *:shared=True --build=missing -pr=profiles/x86_64 -s build_type=Release
conan imports . --install-folder ${BUILD_DIR} --import-folder ${INSTALL_DIR}

cmake -G "Unix Makefiles" \
      -S . \
      -B ${BUILD_DIR} \
      -D CMAKE_BUILD_TYPE=Debug

cmake --build ${BUILD_DIR} -j6 --verbose
cmake --install ${BUILD_DIR} --prefix ${INSTALL_DIR}
