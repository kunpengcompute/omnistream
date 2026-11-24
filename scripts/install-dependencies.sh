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

set -euo pipefail

readonly BUILD_PARALLELISM="${BUILD_PARALLELISM:-32}"

readonly -a supported_install=(
    llvm-15.0.4
    googletest-1.11.0
    jemalloc-5.3.0
    nlohmann-json-3.11.3
    snappy-1.1.10
    rocksdb-8.11.4
    xxhash-0.8.2
    libboundscheck-1.1.16
    boostkit-ksl-2.5.1
    boostkit-kaccjson-1.1.0
    abseil-cpp-20250127.0
    re2-2024-07-02
    rapidjson-master
    librdkafka-2.6.1
    bisheng-jdk-17.0.18
    flink-1.16.3
    nexmark-master
    omniadaptor-master
    omnioperator-master
)
readonly -a supported_setup=(maven)

usage() {
    cat <<'EOF'
Usage:
  bash scripts/install-dependencies.sh --install <component>
  bash scripts/install-dependencies.sh --setup <type>
  bash scripts/install-dependencies.sh --help

Options:
  --install <component>      Install one component
  --setup <type>             Apply one configuration type
  -h, --help                 Show this help message

Supported install targets:
EOF
    printf '  %s\n' "${supported_install[@]}"
    printf '\nSupported setup types:\n'
    printf '  %s\n' "${supported_setup[@]}"
}

is_supported() {
    local target="$1"
    shift
    local supported
    for supported in "$@"; do
        if [[ "$target" == "$supported" ]]; then
            return 0
        fi
    done
    return 1
}

install_llvm_15_0_4() {
    mkdir -p /opt/buildtools && cd /opt/buildtools
    git clone --depth 1 --branch llvmorg-15.0.4 https://gitcode.com/GitHub_Trending/ll/llvm-project.git
    mkdir -p "/opt/buildtools/llvm-project/build" && cd "/opt/buildtools/llvm-project/build"
    # Preserve the original LLVM target option; BPE may have been intended as BPF.
    cmake -G "Unix Makefiles" -DLLVM-TARGETS_TO_BUILD="host;ARM;X86;AArch64;BPE" -DCMAKE_BUILD_TYPE=Release -DLLVM_BUILD_LLVM_DYLIB=true -DLLVM_ENABLE_RTTI=ON -DLLVM_ENABLE_PROJECTS="clang;lld;libunwind;compiler-rt;lldb" -DCMAKE_INSTALL_PREFIX=/usr/local/ ../llvm
    make -j"$BUILD_PARALLELISM" && make install

    echo 'export LIBRARY_PATH=/usr/local/lib:$LIBRARY_PATH' >> /etc/profile
    echo 'export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH' >> /etc/profile
}

install_googletest_1_11_0() {
    mkdir -p /opt/buildtools && cd /opt/buildtools
    git clone --depth 1 --branch release-1.11.0 https://atomgit.com/GitHub_Trending/go/googletest.git
    cd /opt/buildtools/googletest
    mkdir -p /opt/buildtools/googletest/build && cd /opt/buildtools/googletest/build
    cmake ..
    make -j"$BUILD_PARALLELISM"
    make install
}

install_jemalloc_5_3_0() {
    mkdir -p /opt/buildtools && cd /opt/buildtools
    git clone --depth 1 --branch 5.3.0 https://atomgit.com/GitHub_Trending/je/jemalloc.git
    cd /opt/buildtools/jemalloc
    ./autogen.sh --disable-initial-exec-tls --with-lg-page=16
    make -j"$BUILD_PARALLELISM" && make install
}

install_nlohmann_json_3_11_3() {
    mkdir -p /opt/buildtools && cd /opt/buildtools
    git clone --depth 1 --branch v3.11.3 https://gitcode.com/GitHub_Trending/js/json.git
    mkdir -p /opt/buildtools/json/build && cd /opt/buildtools/json/build
    cmake ..
    make -j"$BUILD_PARALLELISM" && make install
}

install_snappy_1_1_10() {
    mkdir -p /opt/buildtools && cd /opt/buildtools
    git clone --depth 1 --branch openEuler-24.03-LTS-SP1-release https://atomgit.com/src-openeuler/snappy.git
    cd /opt/buildtools/snappy
    tar -zxvf snappy-1.1.10.tar.gz
    cd snappy-1.1.10 && patch -p1 < ../add-option-to-enable-rtti-set-default-to-current-ben.patch && patch -p1 < ../remove-dependency-on-google-benchmark-and-gmock.patch
    mkdir -p /opt/buildtools/snappy/snappy-1.1.10/build && cd /opt/buildtools/snappy/snappy-1.1.10/build
    cmake -DSNAPPY_BUILD_BENCHMARKS=OFF -DSNAPPY_BUILD_TESTS=OFF -DBUILD_SHARED_LIBS=ON -DCMAKE_POSITION_INDEPENDENT_CODE=ON  ..
    make -j"$BUILD_PARALLELISM" && make install
    echo 'export LIBRARY_PATH=/usr/local/lib64:$LIBRARY_PATH' >> /etc/profile
    echo 'export LD_LIBRARY_PATH=/usr/local/lib64:$LD_LIBRARY_PATH' >> /etc/profile
}

install_rocksdb_8_11_4() {
    mkdir -p /opt/buildtools && cd /opt/buildtools
    git clone --depth 1 --branch v8.11.4 https://gitcode.com/gh_mirrors/ro/rocksdb.git
    mkdir -p /opt/buildtools/rocksdb/build && cd /opt/buildtools/rocksdb/build
    cmake .. -DWITH_SNAPPY=1 -DCMAKE_BUILD_TYPE=Release -DUSE_RTTI=1 -DWITH_GFLAGS=0
    make -j"$BUILD_PARALLELISM" && make install
}

install_xxhash_0_8_2() {
    mkdir -p /opt/buildtools && cd /opt/buildtools
    git clone --depth 1 --branch v0.8.2 https://gitcode.com/gh_mirrors/xx/xxHash.git
    mkdir -p /opt/buildtools/xxHash/cmake_unofficial/build && cd /opt/buildtools/xxHash/cmake_unofficial/build
    cmake ..
    make -j"$BUILD_PARALLELISM" && make install
}

install_libboundscheck_1_1_16() {
    mkdir -p /opt/buildtools && cd /opt/buildtools
    git clone --depth 1 --branch v1.1.16 https://atomgit.com/openeuler/libboundscheck.git
    cd /opt/buildtools/libboundscheck
    make CC=gcc
    echo 'export C_INCLUDE_PATH=/opt/buildtools:/opt/buildtools/libboundscheck/include:$C_INCLUDE_PATH' >> /etc/profile
    echo 'export CPLUS_INCLUDE_PATH=/opt/buildtools:/opt/buildtools/libboundscheck/include:$CPLUS_INCLUDE_PATH' >> /etc/profile
    echo 'export LIBRARY_PATH=/opt/buildtools/libboundscheck/lib:$LIBRARY_PATH' >> /etc/profile
    echo 'export LD_LIBRARY_PATH=/opt/buildtools/libboundscheck/lib:$LD_LIBRARY_PATH' >> /etc/profile
}

install_boostkit_ksl_2_5_1() {
    mkdir -p /opt/buildtools/boostkit-ksl && cd /opt/buildtools/boostkit-ksl
    wget --no-check-certificate 'https://kunpeng-repo.obs.cn-north-4.myhuaweicloud.com/Kunpeng%20BoostKit/Kunpeng%20BoostKit%2025.1.RC1/BoostKit-ksl_2.5.1.zip'
    unzip /opt/buildtools/boostkit-ksl/BoostKit-ksl_*.zip
    rpm -ivh boostkit-ksl-*.aarch64.rpm

    echo 'export C_INCLUDE_PATH=/usr/local/ksl/include:$C_INCLUDE_PATH' >> /etc/profile
    echo 'export CPLUS_INCLUDE_PATH=/usr/local/ksl/include:$CPLUS_INCLUDE_PATH' >> /etc/profile
    echo 'export LIBRARY_PATH=/usr/local/ksl/lib:$LIBRARY_PATH' >> /etc/profile
    echo 'export LD_LIBRARY_PATH=/usr/local/ksl/lib:$LD_LIBRARY_PATH' >> /etc/profile
}

install_boostkit_kaccjson_1_1_0() {
    if [[ ! -r /etc/os-release ]]; then
        echo "Unable to identify the operating system: /etc/os-release is missing." >&2
        exit 1
    fi

    . /etc/os-release
    if [[ "${ID,,}" != "openeuler" ]]; then
        echo "Unsupported operating system: ${ID:-unknown}. Only openEuler 22.03 and 24.03 are supported." >&2
        exit 1
    fi

    case "${VERSION_ID:-}" in
        22.03)
            DOWNLOAD_URL="https://boostkit-bigdata-public.obs.cn-north-4.myhuaweicloud.com/buildcache/Kacc_Json/master/Daily.26.0.0.B002/openEuler22.03_JDK17/BoostKit-kaccjson_1.1.0.zip"
            ;;
        24.03)
            DOWNLOAD_URL="https://boostkit-bigdata-public.obs.cn-north-4.myhuaweicloud.com/buildcache/Kacc_Json/master/Daily.26.0.0.B002/openEuler24.03_JDK8/BoostKit-kaccjson_1.1.0.zip"
            ;;
        *)
            echo "Unsupported openEuler version: ${VERSION_ID:-unknown}. Only 22.03 and 24.03 are supported." >&2
            exit 1
            ;;
    esac

    mkdir -p /opt/buildtools/boostkit-kaccjson && cd /opt/buildtools/boostkit-kaccjson
    wget --no-check-certificate "$DOWNLOAD_URL"
    unzip /opt/buildtools/boostkit-kaccjson/BoostKit-kaccjson_1.1.0.zip

    echo 'export LIBRARY_PATH=/opt/buildtools/boostkit-kaccjson:$LIBRARY_PATH' >> /etc/profile
    echo 'export LD_LIBRARY_PATH=/opt/buildtools/boostkit-kaccjson:$LD_LIBRARY_PATH' >> /etc/profile
}

install_abseil_cpp_20250127_0() {
    mkdir -p /opt/buildtools && cd /opt/buildtools
    git clone --depth 1 --branch 20250127.0 https://gitcode.com/GitHub_Trending/ab/abseil-cpp.git
    mkdir -p /opt/buildtools/abseil-cpp/build && cd /opt/buildtools/abseil-cpp/build
    cmake .. -DCMAKE_CXX_STANDARD=17 -DCMAKE_CXX_STANDARD_REQUIRED=ON -DABSL_PROPAGATE_CXX_STD=ON -DBUILD_SHARED_LIBS=OFF -DCMAKE_POSITION_INDEPENDENT_CODE=ON
    make install -j"$BUILD_PARALLELISM"
}

install_re2_2024_07_02() {
    mkdir -p /opt/buildtools && cd /opt/buildtools
    git clone --depth 1 --branch 2024-07-02 https://gitcode.com/gh_mirrors/re21/re2.git
    mkdir -p /opt/buildtools/re2/build && cd /opt/buildtools/re2/build
    cmake .. -DCMAKE_CXX_STANDARD=17 -DCMAKE_CXX_STANDARD_REQUIRED=ON -DBUILD_SHARED_LIBS=ON -DCMAKE_CXX_FLAGS="-fPIC"
    make install -j"$BUILD_PARALLELISM"
}

install_rapidjson_master() {
    mkdir -p /opt/buildtools && cd /opt/buildtools
    git clone https://gitcode.com/GitHub_Trending/ra/rapidjson.git

    echo 'export C_INCLUDE_PATH=/opt/buildtools/rapidjson/include:$C_INCLUDE_PATH' >> /etc/profile
    echo 'export CPLUS_INCLUDE_PATH=/opt/buildtools/rapidjson/include:$CPLUS_INCLUDE_PATH' >> /etc/profile
}

install_omnioperator_master() {
    mkdir -p /opt/buildtools && cd /opt/buildtools
    git clone https://gitcode.com/openeuler/OmniOperator.git
    mv OmniOperator OmniOperatorJIT && cd /opt/buildtools/OmniOperatorJIT
    export OMNI_HOME=/opt/buildtools/OmniOperatorJIT
    bash /opt/buildtools/OmniOperatorJIT/build_scripts/build.sh release:java --exclude-test
    echo 'export LIBRARY_PATH=/opt/buildtools/OmniOperatorJIT/lib:$LIBRARY_PATH' >> /etc/profile
 	  echo 'export LD_LIBRARY_PATH=/opt/buildtools/OmniOperatorJIT/lib:$LD_LIBRARY_PATH' >> /etc/profile
}

install_librdkafka_2_6_1() {
    local librdkafka_patch_url="https://raw.gitcode.com/openeuler/OmniStream/raw/master/cpp/connector/kafka/omni_kafka_opt.patch"
    local librdkafka_patch_file="$(mktemp)"
    trap "rm -f -- '${librdkafka_patch_file}'" EXIT
    wget --no-check-certificate -O ${librdkafka_patch_file} ${librdkafka_patch_url}
    mkdir -p /opt/buildtools && cd /opt/buildtools
    git clone --depth 1 --branch v2.6.1 https://atomgit.com/GitHub_Trending/li/librdkafka.git
    cd /opt/buildtools/librdkafka
    git apply "$librdkafka_patch_file"
    ./configure --CFLAGS="-O3" --CXXFLAGS="-O3"
    make -j"$BUILD_PARALLELISM" && make install
}

install_bisheng_jdk_17_0_18() {
    mkdir -p /usr/local && cd /usr/local
    JDK_TAR="bisheng-jdk-17.0.18-b13-linux-aarch64.tar.gz"
    wget --no-check-certificate "https://mirrors.huaweicloud.com/kunpeng/archive/compiler/bisheng_jdk/${JDK_TAR}"

    # Read the top-level directory name from the tar archive.
    set +o pipefail
    JDK_DIR=$(tar -tf "${JDK_TAR}" | head -1 | cut -d/ -f1)
    set -o pipefail

    tar -zxvf "/usr/local/${JDK_TAR}"
    chown -R root:root "/usr/local/${JDK_DIR}"
    ln -sfn "/usr/local/${JDK_DIR}" /usr/local/java
    rm -f "/usr/local/${JDK_TAR}"

    echo "JDK installed to: /usr/local/${JDK_DIR}, soft link: /usr/local/java"
    sed -i '/JAVA_HOME/d' /etc/profile
    echo 'export JAVA_HOME=/usr/local/java' >> /etc/profile
    echo 'export PATH=$JAVA_HOME/bin:$PATH' >> /etc/profile
    echo 'export C_INCLUDE_PATH=$JAVA_HOME/include:$JAVA_HOME/include/linux:$C_INCLUDE_PATH' >> /etc/profile
    echo 'export CPLUS_INCLUDE_PATH=$JAVA_HOME/include:$JAVA_HOME/include/linux:$CPLUS_INCLUDE_PATH' >> /etc/profile
    echo 'export LIBRARY_PATH=$JAVA_HOME/lib:$JAVA_HOME/lib/server:$LIBRARY_PATH' >> /etc/profile
    echo 'export LD_LIBRARY_PATH=$JAVA_HOME/lib:$JAVA_HOME/lib/server:$LD_LIBRARY_PATH' >> /etc/profile
}

setup_maven() {
    cat > /etc/maven/settings.xml << EOF
<?xml version="1.0" encoding="UTF-8"?>
<settings xmlns="http://maven.apache.org/SETTINGS/1.2.0"
          xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
          xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.2.0 https://maven.apache.org/xsd/settings-1.2.0.xsd">
    <mirrors>
        <mirror>
            <id>huaweicloud-maven</id>
            <name>Huawei Cloud Maven Repository</name>
            <url>https://repo.huaweicloud.com/repository/maven/</url>
            <mirrorOf>central</mirrorOf>
        </mirror>
    </mirrors>

    <pluginGroups></pluginGroups>
    <servers></servers>
    <profiles>
        <profile>
            <id>huaweicloud</id>
            <activation>
                <activeByDefault>true</activeByDefault>
            </activation>
            <repositories>
                <repository>
                    <id>huaweicloud-maven</id>
                    <name>Huawei Cloud Maven Repository</name>
                    <url>https://repo.huaweicloud.com/repository/maven/</url>
                    <releases>
                        <enabled>true</enabled>
                    </releases>
                    <snapshots>
                        <enabled>true</enabled>
                    </snapshots>
                </repository>
            </repositories>
            <pluginRepositories>
                <pluginRepository>
                    <id>huaweicloud-maven</id>
                    <name>Huawei Cloud Maven Repository</name>
                    <url>https://repo.huaweicloud.com/repository/maven/</url>
                    <releases>
                        <enabled>true</enabled>
                    </releases>
                    <snapshots>
                        <enabled>true</enabled>
                    </snapshots>
                </pluginRepository>
            </pluginRepositories>
        </profile>
    </profiles>

    <activeProfiles>
        <activeProfile>huaweicloud</activeProfile>
    </activeProfiles>
</settings>
EOF
}

install_omniadaptor_master() {
    mkdir -p /opt/buildtools && cd /opt/buildtools
  	git clone https://atomgit.com/openeuler/OmniAdaptor.git
  	cd /opt/buildtools/OmniAdaptor/omnistream/omniop-flink-extension/omni-flink-bundle/
  	mvn clean package -DskipTests -Dmaven.wagon.http.ssl.insecure=true -Dmaven.wagon.http.ssl.allowall=true
}

install_flink_1_16_3() {
    mkdir -p /usr/local && cd /usr/local
    FLINK_TAR="flink-1.16.3-bin-scala_2.12.tgz"
    wget --no-check-certificate "https://mirrors.huaweicloud.com/apache/flink/flink-1.16.3/${FLINK_TAR}"

    # Read the top-level directory name from the tar archive.
    set +o pipefail
    FLINK_DIR=$(tar -tf "${FLINK_TAR}" | head -1 | cut -d/ -f1)
    set -o pipefail

    tar -zxvf "/usr/local/${FLINK_TAR}"
    chown -R root:root "/usr/local/${FLINK_DIR}"
    ln -s "/usr/local/${FLINK_DIR}" /usr/local/flink
    rm -f "/usr/local/${FLINK_TAR}"

    echo "Flink installed to: /usr/local/${FLINK_DIR}, soft link: /usr/local/flink"
    echo 'export FLINK_HOME=/usr/local/flink' >> /etc/profile

    # Install gson-2.11.0 and json-20240303
    cd /usr/local/flink/lib
    wget --no-check-certificate https://repo.maven.apache.org/maven2/org/json/json/20240303/json-20240303.jar
    wget --no-check-certificate https://repo.maven.apache.org/maven2/com/google/code/gson/gson/2.11.0/gson-2.11.0.jar
}

install_nexmark_master() {
    cd /tmp
    git clone https://github.com/nexmark/nexmark.git
    cd /tmp/nexmark/nexmark-flink

    # Target Flink version is 1.16.3 and skip test compilation to allow the build to succeed.
    sed -i 's#<flink.version>.*</flink.version>#<flink.version>1.16.3</flink.version>#' ../pom.xml
    sed -i 's/-DskipTests/-Dmaven.test.skip=true/' build.sh
    bash /tmp/nexmark/nexmark-flink/build.sh

    NEXMARK_TAR="nexmark-flink.tgz"
    cp -f /tmp/nexmark/nexmark-flink/${NEXMARK_TAR} /usr/local/

    # Read the top-level directory name from the tar archive.
    set +o pipefail
    NEXMARK_DIR=$(tar -tf "/usr/local/${NEXMARK_TAR}" | head -1 | cut -d/ -f1)
    set -o pipefail

    tar -zxvf "/usr/local/${NEXMARK_TAR}" -C /usr/local
    chown -R root:root "/usr/local/${NEXMARK_DIR}"
    ln -sfn "/usr/local/${NEXMARK_DIR}" /usr/local/nexmark
    rm -f "/usr/local/${NEXMARK_TAR}"

    echo "Nexmark installed to: /usr/local/${NEXMARK_DIR}, soft link: /usr/local/nexmark"
    echo "export NEXMARK_HOME=/usr/local/nexmark" >> /etc/profile
    cp -f /usr/local/nexmark/lib/* /usr/local/flink/lib
}

main() {
    if [[ $# -eq 1 && ( "$1" == "-h" || "$1" == "--help" ) ]]; then
        usage
        return 0
    fi

    if [[ $# -ne 2 ]]; then
        echo "Expected --install <component> or --setup <type>." >&2
        usage >&2
        return 1
    fi

    local target="$2"
    local component
    case "$1" in
        --install)
            if ! is_supported "$target" "${supported_install[@]}"; then
                echo "Unsupported install target: $target" >&2
                usage >&2
                return 1
            fi
            "install_${target//[.-]/_}"
            ;;
        --setup)
            if ! is_supported "$target" "${supported_setup[@]}"; then
                echo "Unsupported setup type: $target" >&2
                usage >&2
                return 1
            fi
            "setup_${target//[.-]/_}"
            ;;
        *)
            echo "Unknown option: $1" >&2
            usage >&2
            return 1
            ;;
    esac
}

main "$@"
