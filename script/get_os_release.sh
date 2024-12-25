#!/bin/bash

LOG_GET_OS_RELEASE=$1

if [[ ! -f /etc/os-release ]]; then
  echo "[ERROR] os release info not found" 1>&2
  exit 1
fi

function get_os_var() {
  grep "^$1=" /etc/os-release | cut -d '=' -f 2 | tr -d '"'
}

OS_NAME=$(get_os_var "NAME")
OS_ID=$(get_os_var "ID")
OS_VERSION=$(get_os_var "VERSION")
OS_VERSION_ID=$(get_os_var "VERSION_ID")
OS_PRETTY_NAME=$(get_os_var "PRETTY_NAME")

if [ -z "$OS_PRETTY_NAME" ]; then
    OS_PRETTY_NAME="${OS_NAME} ${OS_VERSION}"
fi

OS_RELEASE="$OS_VERSION_ID"

function logger() {
  if [[ "${LOG_GET_OS_RELEASE}" != "OFF" ]]; then
    echo "$@"
  fi
}

function set_compatibility() {
  logger "[NOTICE] '${OS_PRETTY_NAME}' is compatible with CentOS $1, use el$1 dependencies list"
  OS_RELEASE="$1"
}

function compat_centos8() {
  set_compatibility 8
}

function compat_centos7() {
  set_compatibility 7
}

function not_supported() {
  echo "[ERROR] '${OS_PRETTY_NAME}' is not supported yet."
}

function version_ge() {
  test "$(awk -v v1="$OS_VERSION_ID" -v v2="$1" 'BEGIN{print(v1>=v2)?"1":"0"}' 2>/dev/null)" == "1"
}

case "${OS_ID}" in
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
  version_ge "22.04" && compat_centos8 && return
  version_ge "16.04" && compat_centos7 && return
  ;;
centos)
  version_ge "8.0" && OS_RELEASE=8 && return
  version_ge "7.0" && OS_RELEASE=7 && return
  ;;
almalinux)
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
  logger "${OS_PRETTY_NAME} is NOT SUPPORTED for compiling now!!! use el7 for READONLY mode. And GNU grep should be installed: "
  logger "\tbrew install grep"
  export PATH="/usr/local/opt/grep/libexec/gnubin:$PATH"
  compat_centos7 && return
  ;;
esac
not_supported && return 1
