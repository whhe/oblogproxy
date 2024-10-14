#!/bin/bash

if [[ ! -f /etc/os-release ]]; then
  echo "[ERROR] os release info not found" 1>&2
fi
source /etc/os-release || exit 1

PNAME=${PRETTY_NAME:-${NAME} ${VERSION}}
OS_ARCH="$(uname -p)" || exit 1
OS_RELEASE="0"
LOGGER_SWITCH="ON"

function logger() {
  if [[ "$LOGGER_SWITCH" != "OFF" ]];then
    for arg in "$@"; do
      echo "$arg"
    done
  fi
}

function compat_centos9() {
  logger "[NOTICE] '$PNAME' is compatible with CentOS 9, use el9 dependencies list"
  OS_RELEASE=9
}

function compat_centos8() {
  logger "[NOTICE] '$PNAME' is compatible with CentOS 8, use el8 dependencies list"
  OS_RELEASE=8
}

function compat_centos7() {
  logger "[NOTICE] '$PNAME' is compatible with CentOS 7, use el7 dependencies list"
  OS_RELEASE=7
}

function not_supported() {
  echo "[ERROR] '$PNAME' is not supported yet."
}

function version_ge() {
  test "$(awk -v v1="$VERSION_ID" -v v2="$1" 'BEGIN{print(v1>=v2)?"1":"0"}' 2>/dev/null)" == "1"
}

function get_os_release() {
  if [ -z "$1" ]; then
    LOGGER_SWITCH="ON"
  else
    LOGGER_SWITCH="$1"
  fi

  case "$ID" in
  alinux)
    version_ge "3.0" && compat_centos8 && return
    version_ge "2.0" && compat_centos7 && return
    ;;
  alios)
    version_ge "8.0" && compat_centos8 && return
    version_ge "7.2" && compat_centos7 && return
    ;;
  anolis)
    version_ge "8.0" && compat_centos8 && return
    version_ge "7.0" && compat_centos7 && return
    ;;
  ubuntu)
    version_ge "22.04" && compat_centos9 && return
    version_ge "16.04" && compat_centos7 && return
    ;;
  centos)
    version_ge "8.0" && OS_RELEASE=8 && return
    version_ge "7.0" && OS_RELEASE=7 && return
    ;;
  almalinux)
    version_ge "9.0" && compat_centos9 && return
    version_ge "8.0" && compat_centos8 && return
    ;;
  debian)
    version_ge "9" && compat_centos7 && return
    ;;
  fedora)
    version_ge "33" && compat_centos7 && return
    ;;
  opensuse-leap)
    version_ge "15" && compat_centos7 && return
    ;;
  sles)
    version_ge "15" && compat_centos7 && return
    ;;
  uos)
    version_ge "20" && compat_centos7 && return
    ;;
  # not support compiling, but for IDE loading sources
  macOS)
    logger "${ID} $PNAME is NOT SUPPORTED for compiling now!!! use el8.x86_64 for READONLY mode. And GNU grep should be installed: "
    logger "\tbrew install grep"
    OS_ARCH=x86_64
    export PATH="/usr/local/opt/grep/libexec/gnubin:$PATH"
    compat_centos7 && return
    ;;
  esac
  not_supported && return 1
}