#!/bin/bash
# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# omnistream is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan
# PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#         http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
# KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.

set -x
WORKSPACE=$(realpath "$(dirname "$0")")

LLVM_DIR=/opt/buildtools/llvm-15.0.4/lib/cmake/llvm
GCC_INSTALL_PATH=/usr/local/

OMNISTREAM_SRC_PATH=$WORKSPACE
OMNISTREAM_PGO_PROFILE_PATH=$WORKSPACE/profile/flink_omnistream_profile

RDKAFKA_SRC_PATH=$WORKSPACE/3rdparty/rdkafka
RDKAFKA_PGO_PROFILE_PATH=$WORKSPACE/profile/flink_rdkafka_profile

GLOBAL_FLAGS="-O3 -flto=auto -DNDEBUG"
GLOBAL_FLAGS+=" -Wno-missing-profile -Wno-deprecated-declarations -Wno-range-loop-construct -Wno-odr"

function build_omnistream {
    cd $WORKSPACE/profile
    tar zxvf flink_omnistream_profile.tar.gz
    cd flink_omnistream_profile
    srcdir=$(echo "$OMNISTREAM_SRC_PATH" | sed 's/\//#/g')
    rename "#repo#codehub#OmniStream" "$srcdir" *

    rm -rf $OMNISTREAM_SRC_PATH/cpp/cmake_build_release
    mkdir -p $OMNISTREAM_SRC_PATH/cpp/cmake_build_release
    cd $OMNISTREAM_SRC_PATH/cpp/cmake_build_release

    local flags="$GLOBAL_FLAGS -finline-force=libcore.a,libtable.a,libruntime.a,libdatagen.a,libbasictypes.a"
    flags="$flags -fcfgo-profile-use=$OMNISTREAM_PGO_PROFILE_PATH -Wno-error=coverage-mismatch -Wno-error=missing-profile -fprofile-correction -fprofile-update=atomic"

    cmake .. \
        -DLLVM_DIR=$LLVM_DIR \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_C_COMPILER=$GCC_INSTALL_PATH/bin/gcc \
        -DCMAKE_CXX_COMPILER=$GCC_INSTALL_PATH/bin/g++ \
        -DCMAKE_CXX_FLAGS_RELEASE="$flags" \
        -DCMAKE_C_FLAGS="$flags" \
        -DCMAKE_CXX_FLAGS="$flags" \
        -DCMAKE_SHARED_LINKER_FLAGS="$flags" \
        -DCMAKE_AR=$GCC_INSTALL_PATH/bin/gcc-ar \
        -DCMAKE_RANLIB=$GCC_INSTALL_PATH/bin/gcc-ranlib \
        -DENABLE_TESTS=off || {
            echo "CMake Failed"
            exit 1
    }

    sed -i "s/-MD -MT table\/CMakeFiles\/table.dir\/typeutils\/InternalTypeInfo.cpp.o/-fno-lto &/" table/CMakeFiles/table.dir/build.make
    sed -i "s/-MD -MT table\/CMakeFiles\/table.dir\/types\/logical\/LogicalType.cpp.o/-fno-lto &/" table/CMakeFiles/table.dir/build.make
    make  -j$num_cpus  || {
        echo "Make Failed"
        exit 1
    }

    cmake .. -DCMAKE_BUILD_TYPE=release -DCMAKE_INSTALL_PREFIX=$(pwd)/libbasictypes && cmake --build . --parallel 16 --target basictypes && cmake --install .|| {
        echo "CMake Failed"
        exit 1
    }
    cmake .. -DCMAKE_BUILD_TYPE=release -DCMAKE_INSTALL_PREFIX=$(pwd)/libbasictypes && cmake --build . --parallel 16 --target functions && cmake --install .|| {
        echo "CMake Failed"
        exit 1
    }
    cmake .. -DCMAKE_BUILD_TYPE=release -DCMAKE_INSTALL_PREFIX=$(pwd)/libbasictypes && cmake --build . --parallel 16 --target thirdlibrary && cmake --install .|| {
        echo "CMake Failed"
        exit 1
    }
}

function build_rdkafka {
    cd $WORKSPACE/profile/
    tar zxvf flink_rdkafka_profile.tar.gz
    cd flink_rdkafka_profile
    srcdir=$(echo "$RDKAFKA_SRC_PATH" | sed 's/\//#/g')
    rename "#repo#codehub#OmniStream#3rdparty#rdkafka" "$srcdir" *

    cd $RDKAFKA_SRC_PATH
    git apply "${OMNISTREAM_SRC_PATH}/cpp/connector/kafka/omni_kafka_opt.patch"
    ./configure --cc=$GCC_INSTALL_PATH/bin/gcc --cxx=$GCC_INSTALL_PATH/bin/g++ --CFLAGS="-O3" --CXXFLAGS="-O3"

    GCC_INSTALL_PATH_CHANGED=$(echo "$GCC_INSTALL_PATH" | sed 's/\//\\\//g')
    sed -i "s/^RANLIB=.*/RANLIB=$GCC_INSTALL_PATH_CHANGED\/bin\/gcc-ranlib/" Makefile.config
    echo "AR=$GCC_INSTALL_PATH/bin/gcc-ar" >> Makefile.config

    local flags="$GLOBAL_FLAGS -Wall -Wsign-compare -Wfloat-equal -Wpointer-arith -Wcast-align -Wno-free-nonheap-object -fPIC"
    local cflags="$flags -fcfgo-profile-use=$RDKAFKA_PGO_PROFILE_PATH -Wno-error=coverage-mismatch -Wno-error=missing-profile -fprofile-correction -fprofile-update=atomic"

    flags_changed=$(echo "$flags" | sed 's/\//\\\//g')
    cflags_changed=$(echo "$cflags" | sed 's/\//\\\//g')
    sed -i "s/^CPPFLAGS+=.*/CPPFLAGS+= $flags_changed/" Makefile.config
    sed -i "s/^CFLAGS+=.*/CFLAGS+= $cflags_changed/" Makefile.config
    sed -i "71i\\LIB_LDFLAGS+= ${cflags_changed}" src/Makefile

    make -j`nproc` LDFLAGS="$flags" RANLIB=$GCC_INSTALL_PATH/bin/gcc-ranlib AR=$GCC_INSTALL_PATH/bin/gcc-ar libs
    sudo make install

    sed -i '71d' src/Makefile
}

if [ "$1" == "omnistream" ];then
    build_omnistream
elif [ "$1" == "rdkafka" ];then
    build_rdkafka
else
    exit 1
fi
