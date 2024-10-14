#!/bin/bash

CUR_DIR=$(dirname $(readlink -f "$0"))
PROJECT_DIR=${1:-${CUR_DIR}/../}
PROJECT_NAME=$2
VERSION=$3
RELEASE=$4
CPU_CORES=2 # to avoid oom in build package environment
echo "[BUILD] args: CURDIR=${CUR_DIR} PROJECT_NAME=${PROJECT_NAME} VERSION=${VERSION} RELEASE=${RELEASE}"

# init build env
source ${PROJECT_DIR}/script/init_build_env_ce.sh

# inject env variables
export PROJECT_NAME=${PROJECT_NAME}
export VERSION=${VERSION}
export RELEASE=${RELEASE}

# prepare building env
cd $CUR_DIR
DEP_DIR=$CUR_DIR/deps
mkdir -p $DEP_DIR
OS_ARCH=$(uname -m)
OS_RELEASE=$(grep -Po '(?<=release )\d' /etc/redhat-release)
OS_TAG=${OS_ARCH}/${OS_RELEASE}

# build rpm
cd $PROJECT_DIR
rm -rf build_rpm
mkdir build_rpm
cd build_rpm
${CMAKE_COMMAND} .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_VERBOSE_MAKEFILE=ON -DWITH_DEBUG=OFF -DWITH_US_TIMESTAMP=ON -D OBLOGPROXY_PACKAGE_NAME=${PROJECT_NAME}
${CMAKE_COMMAND} --build . --target package -j ${CPU_CORES}

# archiving artifacts
cd $CUR_DIR
find ${PROJECT_DIR}/build_rpm -name "*.rpm" -maxdepth 1 -exec mv {} . 2>/dev/null \;