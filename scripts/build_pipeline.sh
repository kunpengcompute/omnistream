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

#

set -e
set -x
# Get the absolute directory of the script
script_dir=$(dirname "$(readlink -f "$0")")

# Get the parent directory of the script directory
project_root="$(dirname "$script_dir")"
OMNI_HOME="$project_root"
mkdir -p $OMNI_HOME/lib
mkdir -p $OMNI_HOME/include

dependency_root="$(realpath  "$project_root/3rdparty")"
mkdir -p $dependency_root

# todo: test
export HOME=$project_root/3rdparty

# Define the cpp_root and java_master_dir
cpp_root="$project_root/cpp"
java_master_dir="$project_root/omni-flink-bundle"

echo "Script directory: $script_dir"
echo "Project root: $project_root"
echo "CPP root: $cpp_root"
echo "Java master directory: $java_master_dir"
echo "Dependencies source tarball directory: $dependency_root"

num_cpus=${num_cpus:-16}
echo "Number of CPUs: $num_cpus"

# Function to show usage
function show_usage() {
    echo "Usage: $0 [options]"
    echo "Options:"
    echo "  -h, --help        Show this help message"
    echo "  -c, --cpp  mode       Build the C++ project"
    echo "  -j, --java        Build the Java project"
    echo "  -u, --unit-test   Build all (C++, Java, Dependencies) And Run unit tests"
    echo "  -a, --all  mode       Build all (C++, Java, Dependencies)"
    echo "  -g, --gccopt  mode    Build all (C++, Java, Dependencies) with gcc optimization"
}

function build_boundscheck() {
    echo "test"
  local boundscheck_src_dir="$dependency_root/libboundscheck"

  echo "Start build open source code for libboundscheck"
  cd ${boundscheck_src_dir}/src
  sudo make

  export C_INCLUDE_PATH="$dependency_root:$C_INCLUDE_PATH"
  export CPLUS_INCLUDE_PATH="$dependency_root:$CPLUS_INCLUDE_PATH"
  export LD_LIBRARY_PATH="${boundscheck_src_dir}/lib:$LD_LIBRARY_PATH"
  export LIBRARY_PATH="${boundscheck_src_dir}/lib:$LIBRARY_PATH"
}

function build_java() {
    echo "Building java extension"
    cd "$java_master_dir"
    mvn clean package -Dmaven.wagon.http.ssl.insecure=true || {
        echo "Error: Failed to build Java project"
        exit 1
    }
}

function build_json() {
   json_src_dir="$dependency_root/json"
   cd $json_src_dir
   mkdir -p build
   cd build
   cmake ..
   make -j$num_cpus
   sudo make install
}

function build_google_test() {
   dep_src_dir="$dependency_root/googletest"
   cd  $dep_src_dir
   mkdir -p build
   cd build
   cmake ..
make -j$num_cpus
   sudo make install
}

function build_jemalloc() {
   dep_src_dir="$dependency_root/jemalloc"
   cd  $dep_src_dir
   ./autogen.sh --disable-initial-exec-tls
   make -j$num_cpus
   sudo make install
}

function build_rdkafka() {
   dep_src_dir="$dependency_root/rdkafka"
   cd  $dep_src_dir
   git apply "${cpp_root}/connector/kafka/omni_kafka_opt.patch"
   ./configure --CFLAGS="-O3" --CXXFLAGS="-O3"
   make -j$num_cpus
   sudo make install
}

function build_rdkafka_gccopt() {
   sh $script_dir/build_gcc.sh rdkafka
}

function build_abseil() {
   dep_src_dir="$dependency_root/abseil-cpp"
   cd  $dep_src_dir
   mkdir -p build
   cd build
   cmake .. -DBUILD_SHARED_LIBS=ON
   make -j$num_cpus
   sudo make install
}

function build_re2() {
   dep_src_dir="$dependency_root/re2"
   cd  $dep_src_dir
   mkdir -p build
   cd build
   cmake .. -DBUILD_SHARED_LIBS=ON
   make -j$num_cpus
   sudo make install
}


function build_xxhash() {
   dep_src_dir="$dependency_root/xxHash"
   cd  $dep_src_dir
   mkdir -p build
   cd build
   cmake ../cmake_unofficial
   cmake --build .
   sudo cmake --build . --target install
}


function build_gflags() {
   dep_src_dir="$dependency_root/gflags"
   cd  $dep_src_dir
   mkdir -p build
   cd build
   cmake -DBUILD_SHARED_LIBS=ON ..
   make -j$num_cpus
   sudo make install
}

function build_snappy() {
   dep_src_dir="$dependency_root/snappy"
   cd  $dep_src_dir
   tar -zxvf snappy-1.1.10.tar.gz
   cd snappy-1.1.10
   patch -p1 < ../add-option-to-enable-rtti-set-default-to-current-ben.patch
   patch -p1 < ../remove-dependency-on-google-benchmark-and-gmock.patch
   mkdir -p build
   cd build
   cmake -DSNAPPY_BUILD_BENCHMARKS=OFF -DSNAPPY_BUILD_TESTS=OFF -DBUILD_SHARED_LIBS=ON -DCMAKE_POSITION_INDEPENDENT_CODE=ON  ..
   make -j$num_cpus
   sudo make install
}

function build_rocksdb() {
   dep_src_dir="$dependency_root/rocksdb"
   cd  $dep_src_dir
   mkdir -p build
   cd build
   cmake .. -DWITH_SNAPPY=1 -DCMAKE_BUILD_TYPE=Release -DUSE_RTTI=1 -DWITH_GFLAGS=0
   make -j$num_cpus
   sudo make install
}

# llvm installation takes too much time, we have installed it as part of the compile tools
#function build_llvm() { }

function build_cpp() {
  # Default build mode if no arguments are provided
  build_mode="release"
  build_type=Release
  cpp_build_dir="$cpp_root/cmake_build_release"
  enable_tests="OFF"

  # Check if arguments are provided
  if [[ $# -gt 0 ]]; then
    build_mode=$1
    enable_tests=$2
    shift 1
  fi

  # Call the appropriate build function based on the build mode
  case "$build_mode" in
    debug)
	build_type=Debug
	cpp_build_dir="$cpp_root/cmake_build_debug"
	#cmake commend is : cmake -DCMAKE_BUILD_TYPE=Debug  -DBUILD_SHARED_LIBS=ON  ..
	build_with_mode $build_mode $cpp_build_dir $build_type $enable_tests
        ;;
    coverage)
  cpp_build_dir="$cpp_root/cmake_build_coverage"
  build_with_mode COVERAGE_TEST $cpp_build_dir $build_type $enable_tests
        ;;
    perf)
	build_type=PERF_TEST
	cpp_build_dir="$cpp_root/cmake_build_perf"
	build_with_mode $build_mode $cpp_build_dir $build_type $enable_tests
        ;;
    release)
	build_with_mode $build_mode $cpp_build_dir $build_type $enable_tests
        ;;
    *)
        echo "Invalid build mode. Valid options are: debug, perf, release"
        exit 1
        ;;
  esac
}

function build_cpp_gccopt() {
    sh $script_dir/build_gcc.sh omnistream
}

# Function to build in  mode
# $1 mode
# $2 cpp_build_dir
# $3 build type
# $4 whether to enable unit tests

function build_with_mode() {
    # Build command with debug flags and additional arguments

    echo "$(date)" Build with "$@"
    echo "$(date)" with mode "$1"
    mode=$1
    cpp_build_dir=$2
    build_type=$3
    enable_tests=$4

    mkdir -p $cpp_build_dir
    cd $cpp_build_dir
    cmake -G Ninja \
      -DCMAKE_BUILD_TYPE=$mode \
      -DLLVM_DIR=/opt/buildtools/llvm-15.0.4/lib/cmake/llvm \
      -DENABLE_TESTS=$enable_tests \
      -DCMAKE_INSTALL_PREFIX="$cpp_build_dir/libbasictypes" \
      -DCMAKE_C_COMPILER_LAUNCHER=ccache \
      -DCMAKE_CXX_COMPILER_LAUNCHER=ccache \
      .. || {
        echo CMake Failed
        exit 1
    }

    cmake --build . --parallel "$num_cpus" || {
      echo Cmake Build Failed
      exit 1
    }

    cmake --install . || {
      echo CMake Install Failed
      exit 1
    }

    echo "$(date)" Finish build with "$@"
}

# Function to run unit test under  mode
# $1 mode
# $2 cpp_build_dir
# $3 build type

function run_test_with_mode() {
  echo run test   with "$@"
  echo run test with mode "$1"

  # Default build mode if no arguments are provided
  build_mode="release"
  build_type=Release
  cpp_build_dir="$cpp_root/cmake_build_release"

  # Check if arguments are provided
  if [[ $# -gt 0 ]]; then
    build_mode=$1
    shift 1
  fi

  # Call the appropriate build function based on the build mode
  case "$build_mode" in
    debug)
	build_type=Debug
	cpp_build_dir="$cpp_root/cmake_build_debug"
        ;;
    coverage)
  build_type=Coverage
  cpp_build_dir="$cpp_root/cmake_build_coverage"
        ;;
    perf)
	build_type=PERF_TEST
	cpp_build_dir="$cpp_root/cmake_build_perf"
        ;;
    release)
        ;;
    *)
        echo "Invalid build mode. Valid options are: debug, perf, release"
        exit 1
        ;;
  esac

    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$cpp_build_dir/jni
    mkdir -p $cpp_build_dir/test
    cd $cpp_build_dir/test
    ./tneltest
    # coverage report
    cd $cpp_build_dir
        fastcov \
            -d ${cpp_root}/ \
            -o table.info \
            --lcov \
            --include "${cpp_root}/" \
            --exclude "/usr/include/" \
            --branch-coverage \
            -j $num_cpus
        genhtml table.info --branch-coverage --output-directory out
        cd ../../
        mkdir -p test_coverage
        cp -r $cpp_build_dir/out/index.html test_coverage/index.html
    if [ $? -ne 0 ];then
      echo "Error: Failed to run unit test"
      exit 1
    fi
}


export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib64/
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$JAVA_HOME/jre/lib/aarch64/server/:$JAVA_HOME/jre/lib/aarch64/
export CPLUS_INCLUDE_PATH=$CPLUS_INCLUDE_PATH:$JAVA_HOME/include:$JAVA_HOME/include/linux
# Parse command-line arguments
if  [[ $# -gt 0 ]]; then
    case "$1" in
        -h|--help)
            show_usage
            exit 0
            ;;
        -c|--cpp)
            # Build C++ dependency
            build_rdkafka
            # Build C++ project (implement this function)
            echo "Building C++ project..."
	          build_cpp $2 "OFF"
            ;;
        -j|--java)
            ;;
        -u|--unit-test)
            build_rdkafka
            build_cpp coverage "ON"
            run_test_with_mode coverage
            ;;
        -a|--all)
            # Build C++, Java, dependencies, no need to build googletest dependency
            build_rdkafka
            build_cpp $2 "OFF"
            ;;
        -g|--gccopt)
            # Build C++, Java, dependencies, no need to build googletest dependency
            build_rdkafka_gccopt
            build_cpp_gccopt $2 "OFF"
            ;;
        *)
            echo "Invalid option: $1"
            show_usage
            exit 1
            ;;
    esac
fi


#end
