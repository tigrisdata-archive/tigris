#!/bin/bash

# This intended to be run duing Docker build only

set -e

ARCH=$(dpkg --print-architecture)

mkdir -p /usr/local
wget "https://go.dev/dl/go1.18.3.linux-${ARCH}.tar.gz"
tar -C /usr/local -xzf "go1.18.3.linux-${ARCH}.tar.gz"
rm "go1.18.3.linux-${ARCH}.tar.gz"
export PATH=$PATH:/usr/local/go/bin
