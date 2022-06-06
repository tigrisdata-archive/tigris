FROM --platform=linux/arm64/v8 ubuntu:latest as build

ENV SOURCE=https://github.com/rbarabas/foundationdb
ENV VERSION=rb/7.1.7_arm64
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
    libjemalloc-dev \
    liblz4-dev \
    libssl-dev \
    mono-devel \
    ninja-build \
    openjdk-11-jdk \
    openssl \
    python3 \
    ruby-dev

FROM build as devel

# Check out repository
RUN git clone ${SOURCE} && \
    cd foundationdb && \
    git checkout ${VERSION} && \
    cd -

# Build
RUN mkdir build_output
ENV CXX_FLAGS="--param ggc-min-expand=20"
RUN cmake -S foundationdb -B build_output -G Ninja
RUN ninja -j3 -C build_output

FROM devel as pkg
COPY --from=devel /build_output /build_output
RUN cd build_output && cpack -G DEB
RUN mkdir /packages
RUN cp build_output/packages/*.deb* /packages

FROM --platform=linux/arm64/v8 ubuntu:latest as release
RUN mkdir /packages
COPY --from=devel /build_output/packages/* /packages
RUN dpkg -i ~/packages/foundationdb-clients*.deb
RUN mkdir /var/lib/foundationdb
RUN chown foundationdb:foundationdb /var/lib/foundationdb
RUN dpkg -i ~/packages/foundationdb-server.deb
RUN rm -rf /packages