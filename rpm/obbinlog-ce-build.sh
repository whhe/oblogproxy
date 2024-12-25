#!/bin/bash

set -x

if [ $# -lt 4 ]; then
    exit 1
fi

CUR_DIR=$(dirname $(readlink -f "$0"))
PROJECT_DIR=${1:-${CUR_DIR}/../}
PACKAGE_NAME=$2
VERSION=$3
RELEASE=$4

TYPE=RPM
COMMUNITY_BUILD=ON
BUILD_TYPE=RELEASE
CONCURRENT=${CONCURRENT:-4}

bash "${CUR_DIR}"/obbinlog-build.sh "${PROJECT_DIR}" "${PACKAGE_NAME}" "${VERSION}" "${RELEASE}" "${TYPE}" "${COMMUNITY_BUILD}" "${BUILD_TYPE}" "${CONCURRENT}"
