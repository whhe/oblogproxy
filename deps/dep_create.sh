#!/bin/bash

function locate_deps_file() {
  source "${PWD}"/../script/get_os_release.sh
  OS_ARCH=$(uname -m)

  OS_TAG="el$OS_RELEASE.$OS_ARCH"
  DEP_FILE="${PWD}/oblogproxy.${OS_TAG}.deps"

  echo -e "check dependencies profile for ${OS_TAG}... \c"
  if [[ ! -f "${DEP_FILE}" ]]; then
    echo "NOT FOUND" 1>&2
    exit 2
  else
    echo "[${DEP_FILE}] FOUND"
  fi
}

function extract_rpm_repo() {
  REPO_NAME=$1
  echo -e "check repository <${REPO_NAME}> address in profile ${DEP_FILE} ... \c"
  RPM_REPO="$(grep -Po "(?<=${REPO_NAME}=).*" "${DEP_FILE}" 2>/dev/null)"
  if [[ $? -eq 0 ]]; then
    echo "$RPM_REPO"
  else
    echo "NOT FOUND" 1>&2
    exit 2
  fi
}

function download_and_install_rpm_packages() {
  RPM_PACKAGES=$1
  REPO=$2
  INSTALL_DIR=$3
  echo "download dependencies from repo <${REPO}>, and will install to <${INSTALL_DIR}> ..."
  for pkg in ${RPM_PACKAGES}; do
    if [[ -f "${TARGET_DIR}/pkg/${pkg}" ]]; then
      echo -e "find package <${pkg}> in cache... \c"
      # cd ${TARGET_DIR} && rpm2cpio "${TARGET_DIR}/pkg/${pkg}" | cpio -di -u --quiet
    else
      echo -e "download package <${pkg}>... \c"
      TEMP=$(mktemp -u ".${pkg}.XXXX")
      wget "${REPO}/${pkg}" -q -O "${TARGET_DIR}/pkg/${TEMP}"

      if [[ $? -eq 0 ]]; then
        mv -f "${TARGET_DIR}/pkg/$TEMP" "${TARGET_DIR}/pkg/${pkg}"
        echo "SUCCESS"
      else
        rm -rf "${TARGET_DIR}/pkg/$TEMP"
        echo "FAILED" 1>&2
        exit 3
      fi

      echo -e "unpack package <${pkg}>... \c"
      cd ${INSTALL_DIR} && rpm2cpio "${TARGET_DIR}/pkg/${pkg}" | cpio -di -u --quiet
    fi

    if [ $? -eq 0 ]; then
      echo "SUCCESS"
    else
      echo "FAILED" 1>&2
      exit 3
    fi
  done
}

function check_matched_count() {
  count=$(echo $1 | wc -l)
  if [ ${count} -gt $2 ]; then
    echo "[ERROR] multiple rpm package, should: $2."
    exit 4
  fi
}

function install_tool_rpm() {
  line=$(grep -n "\[tools\]" ${DEP_FILE} | awk -F ':' '{print $1}')
  RPM_TOOL_FILES=$(awk "NR>${line}" ${DEP_FILE} | sed '/^[  ]*$/d')

  echo -n "dep_create.sh in ${PWD}, target dir: ${TARGET_DIR}, install mode: tool, "
  if [ ! -z ${RPM_NAME} ]; then
    echo "expected rpm name: ${RPM_NAME}"
    RPM_TOOL_FILES=$(echo "${RPM_TOOL_FILES}" | awk -F ' ' '{print $1}' | grep "^${RPM_NAME}")
    check_matched_count RPM_TOOL_FILES 1
  else
    echo "expected rpm name: ALL"
  fi

  extract_rpm_repo "kit_repo"
  download_and_install_rpm_packages "${RPM_TOOL_FILES}" ${RPM_REPO} ${TARGET_DIR}
}

function install_cdc_rpm() {
  line1=$(grep -n "\[deps\]" ${DEP_FILE} | awk -F ':' '{print $1}')
  line2=$(grep -n "\[tools\]" ${DEP_FILE} | awk -F ':' '{print $1}')
  RPM_CDC_FILES=$(awk "NR>${line1} && NR<${line2}" ${DEP_FILE} | sed '/^[  ]*$/d')

  echo -n "dep_create.sh in ${PWD}, target dir: ${TARGET_DIR}, install mode: cdc, "
  if [ ! -z ${RPM_NAME} ] && [ ! -z ${RPM_MAJOR_VERSION} ] && [ ! -z ${RPM_INSTALL_DIR} ]; then
    echo "expected rpm name: ${RPM_NAME}, expected rpm version: ${RPM_MAJOR_VERSION}, expected rpm install dir: ${RPM_INSTALL_DIR}"
    RPM_NAME_WITH_VERSION="${RPM_NAME}-${RPM_MAJOR_VERSION}"
    RPM_CDC_FILE=$(echo "${RPM_CDC_FILES}" | awk -F ' ' '{print $1}' | grep "^${RPM_NAME_WITH_VERSION}")
    check_matched_count RPM_CDC_FILE 1
  else
    echo "rpm name and version is required." 1>&2
    exit 4
  fi

  case "${RPM_MAJOR_VERSION}" in
  2)
    extract_rpm_repo "taobao_repo"
    ;;
  3)
    extract_rpm_repo "stable_repo"
    ;;
  4)
    extract_rpm_repo "oceanbase_cdc_repo"
    ;;
  *)
    extract_rpm_repo "oceanbase_cdc_repo"
    ;;
  esac

  download_and_install_rpm_packages ${RPM_CDC_FILE} ${RPM_REPO} ${RPM_INSTALL_DIR}
}

# 0. clear env
unalias -a

# 1. parse args
COMMUNITY_BUILD=$1
INSTALL_MODE=$2
DEP_DIR=$3
RPM_NAME=$4
RPM_MAJOR_VERSION=$5
RPM_INSTALL_DIR=$6

PWD="$(
  cd $(dirname $0)
  pwd
)"
TARGET_DIR=${PWD}
if [[ ! -z "$DEP_DIR" ]]; then
  TARGET_DIR=$DEP_DIR
  mkdir -p ${TARGET_DIR}
fi
mkdir -p "${TARGET_DIR}/pkg" >/dev/null 2>&1

if [[ ! -z "$RPM_INSTALL_DIR" ]]; then
  mkdir -p "${RPM_INSTALL_DIR}" >/dev/null 2>&1
fi
echo "parsed args, COMMUNITY_BUILD: $COMMUNITY_BUILD, MODE: ${INSTALL_MODE}, TARGET_DIR: ${TARGET_DIR}, RPM_NAME: ${RPM_NAME}, RPM_MAJOR_VERSION: ${RPM_MAJOR_VERSION}, RPM_INSTALL_DIR: ${RPM_INSTALL_DIR}"

# 2. locate deps file
locate_deps_file || exit 1

# 3. install rpm
case "${INSTALL_MODE}" in
tool)
  install_tool_rpm
  ;;
cdc)
  install_cdc_rpm
  ;;
esac
