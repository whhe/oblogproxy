#!/bin/bash

PWD="$(
  cd $(dirname $0)
  pwd
)"

function locate_deps_file() {
  source "${PWD}"/../script/get_os_release.sh OFF
  OS_ARCH=$(uname -m)

  OS_TAG="el$OS_RELEASE.$OS_ARCH"
  DEP_FILE="${PWD}/oblogproxy.${OS_TAG}.deps"
  if [[ ! -f "${DEP_FILE}" ]]; then
    echo "NOT FOUND" 1>&2
    exit 2
  else
    echo ${DEP_FILE}
  fi
}
locate_deps_file