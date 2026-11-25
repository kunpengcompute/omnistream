# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# omnistream is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan
# PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#         http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
# KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.

# This Dockerfile is used to build the compilation environment
ARG BASE_IMAGE
FROM ${BASE_IMAGE}

ARG BUILD_PARALLELISM=32

# Install packages needed to build OmniStream and its source dependencies.
RUN echo 'sslverify=0' >> /etc/yum.conf && \
    yum update -y && \
    yum install -y \
        autoconf boost-devel cmake curl dos2unix file findutils gcc gcc-c++ gdb git \
        glibc-locale-source iproute iputils make maven net-tools ninja-build \
        openssh-clients openssh-server openssl passwd patch perf sudo unzip vim \
        wget xz zip zlib && \
    yum clean all && \
    rm -rf /var/cache/yum/*

# Set the system locale to prevent garbled text.
RUN localedef --no-archive -i en_US -f UTF-8 en_US.UTF-8 && \
    echo 'LANG="en_US.UTF-8"' > /etc/locale.conf
ENV LANG=en_US.UTF-8

# Ignore SSL certificate verification in Git
RUN git config --global http.sslVerify false

COPY install-dependencies.sh /root/scripts/install-dependencies.sh
SHELL ["/bin/bash", "-lc"]

# Install dependencies.
RUN bash /root/scripts/install-dependencies.sh --install llvm-15.0.4
RUN bash /root/scripts/install-dependencies.sh --install googletest-1.11.0
RUN bash /root/scripts/install-dependencies.sh --install jemalloc-5.3.0
RUN bash /root/scripts/install-dependencies.sh --install nlohmann-json-3.11.3
RUN bash /root/scripts/install-dependencies.sh --install snappy-1.1.10
RUN bash /root/scripts/install-dependencies.sh --install rocksdb-8.11.4
RUN bash /root/scripts/install-dependencies.sh --install xxhash-0.8.2
RUN bash /root/scripts/install-dependencies.sh --install libboundscheck-1.1.16
RUN bash /root/scripts/install-dependencies.sh --install boostkit-ksl-2.5.1
RUN bash /root/scripts/install-dependencies.sh --install boostkit-kaccjson-1.1.0
RUN bash /root/scripts/install-dependencies.sh --install abseil-cpp-20250127.0
RUN bash /root/scripts/install-dependencies.sh --install re2-2024-07-02
RUN bash /root/scripts/install-dependencies.sh --install rapidjson-master
RUN bash /root/scripts/install-dependencies.sh --install librdkafka-2.6.1
RUN bash /root/scripts/install-dependencies.sh --install bisheng-jdk-17.0.18
RUN bash /root/scripts/install-dependencies.sh --setup maven
RUN bash /root/scripts/install-dependencies.sh --install omnioperator-master

# Recommended ports to expose: 22 SSH; 8081 Flink UI; 5005/5006 JVM remote debugging.
EXPOSE 22 8081 5005 5006

ENTRYPOINT ["/bin/bash"]
