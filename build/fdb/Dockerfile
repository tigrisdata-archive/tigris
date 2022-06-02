FROM --platform=linux/arm64/v8 ubuntu:latest as build

ENV SOURCE=https://github.com/apple/foundationdb/
ENV VERSION=7.1.7
ENV DEBIAN_FRONTEND=noninteractive

# Setup TZ
RUN ln -fs /usr/share/zoneinfo/America/New_York /etc/localtime

# Install requirements
RUN apt update && apt install -y \
    git \
    vim \
    clang \
    cmake \
    golang \
    libjemalloc2 \
    liblz4-dev \
    libssl-dev \
    mono-devel \
    ninja-build \
    openjdk-11-jdk \
    openssl \
    python3 \
    ruby-dev

# Check out repository
RUN git clone ${SOURCE} && \
    cd foundationdb && \
    git checkout ${VERSION} && \
    cd -

# Build
RUN mkdir build_output
RUN cmake -S foundationdb -B build_output -G Ninja
RUN ninja -C build_output

