# Build setup
FROM --platform=linux/arm64/v8 ubuntu:latest as build
ENV DEBIAN_FRONTEND=noninteractive
RUN ln -fs /usr/share/zoneinfo/America/New_York /etc/localtime
RUN apt update && apt install -y \
    git \
    vim \
    clang \
    cmake \
    golang \
    libjemalloc2 \
    libjemalloc-dev \
    liblz4-dev \
    libssl-dev \
    mono-devel \
    ninja-build \
    openjdk-11-jdk \
    openssl \
    python3 \
    ruby-dev


# Compiling and packaging
FROM build as compile
ENV SOURCE=https://github.com/rbarabas/foundationdb
ENV VERSION=rb/7.1.7_arm64
RUN git clone ${SOURCE} && \
    cd foundationdb && \
    git checkout ${VERSION} && \
    cd -
RUN mkdir build && cmake -S foundationdb -B build -G Ninja
WORKDIR build
RUN ninja -j3
RUN cpack -G DEB


# Release container
FROM --platform=linux/arm64/v8 ubuntu:latest as release
COPY --from=compile /build/packages/foundationdb-clients*.deb .
COPY --from=compile /build/packages/foundationdb-server*.deb .
RUN mkdir /var/lib/foundationdb
RUN dpkg -i ./foundationdb-clients*.deb
RUN chown foundationdb:foundationdb /var/lib/foundationdb
RUN dpkg -i ./foundationdb-server*.deb
RUN rm -rf *.deb