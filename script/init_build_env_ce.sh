#!/bin/bash
# 用于初始化开源版本编译环境

PROJECT_DIR=$(dirname $(readlink -f "$0"))/../
ENV_DIR=${PROJECT_DIR}/packenv
ENV_DEP_DIR=${ENV_DIR}/deps

source "${PROJECT_DIR}"/script/get_os_release.sh
get_os_release || exit 1

OS_TAG=${OS_ARCH}/${OS_RELEASE}

# 1. install and config gcc
gcc_done_file=${ENV_DEP_DIR}/gcc.done
if [ ! -f ${gcc_done_file} ]; then
  bash ${PROJECT_DIR}/deps/dep_create.sh OFF tool ${ENV_DEP_DIR} obdevtools-gcc9
  cp ${ENV_DEP_DIR}/usr/local/oceanbase/devtools/lib64/libstdc++.so.6.0.28 /usr/lib64
  cd /usr/lib64 && rm -rf libstdc++.so.6 && ln -sf libstdc++.so.6.0.28 libstdc++.so.6
  touch ${gcc_done_file}
fi

# 2. install deps from yum
yum install -y which wget rpm rpm-build cpio make binutils zlib zlib-devel bison swig openssl-devel libcurl-devel java-11-openjdk

# 3. install cmake and flex
case $OS_TAG in
    x86_64/7)
    wget https://mirrors.aliyun.com/oceanbase/development-kit/el/7/x86_64/obdevtools-cmake-3.22.1-22022100417.el7.x86_64.rpm -P $ENV_DEP_DIR
    rpm2cpio ${ENV_DEP_DIR}/obdevtools-cmake-3.22.1-22022100417.el7.x86_64.rpm | (cd ${ENV_DEP_DIR} && cpio -idvm)
    flex_file=obdevtools-flex-2.6.1-32023111015.el7.x86_64.rpm
    wget -q https://mirrors.aliyun.com/oceanbase/development-kit/el/7/x86_64/${flex_file} -O ${ENV_DEP_DIR}/${flex_file}
    rpm2cpio ${ENV_DEP_DIR}/${flex_file} | (cd ${ENV_DEP_DIR} && cpio -idvm)
    ;;
    x86_64/8|x86_64/9)
    wget https://mirrors.aliyun.com/oceanbase/development-kit/el/8/x86_64/obdevtools-cmake-3.22.1-22022100417.el8.x86_64.rpm -P $ENV_DEP_DIR
    rpm2cpio ${ENV_DEP_DIR}/obdevtools-cmake-3.22.1-22022100417.el8.x86_64.rpm | (cd ${ENV_DEP_DIR} && cpio -idvm)
    flex_file=obdevtools-flex-2.6.1-32023111015.el8.x86_64.rpm
    wget -q https://mirrors.aliyun.com/oceanbase/development-kit/el/8/x86_64/${flex_file} -O ${ENV_DEP_DIR}/${flex_file}
    rpm2cpio ${ENV_DEP_DIR}/${flex_file} | (cd ${ENV_DEP_DIR} && cpio -idvm)
    ;;
    aarch64/7)
    wget https://mirrors.aliyun.com/oceanbase/development-kit/el/7/aarch64/obdevtools-cmake-3.22.1-22022100417.el7.aarch64.rpm -P $ENV_DEP_DIR
    rpm2cpio ${ENV_DEP_DIR}/obdevtools-cmake-3.22.1-22022100417.el7.aarch64.rpm | (cd ${ENV_DEP_DIR} && cpio -idvm)
    flex_file=obdevtools-flex-2.6.1-32023111015.el7.aarch64.rpm
    wget -q https://mirrors.aliyun.com/oceanbase/development-kit/el/7/aarch64/${flex_file} -O ${ENV_DEP_DIR}/${flex_file}
    rpm2cpio ${ENV_DEP_DIR}/${flex_file} | (cd ${ENV_DEP_DIR} && cpio -idvm)
    ;;
    aarch64/8|aarch64/9)
    wget https://mirrors.aliyun.com/oceanbase/development-kit/el/8/aarch64/obdevtools-cmake-3.22.1-22022100417.el8.aarch64.rpm -P $ENV_DEP_DIR
    rpm2cpio ${ENV_DEP_DIR}/obdevtools-cmake-3.22.1-22022100417.el8.aarch64.rpm | (cd ${ENV_DEP_DIR} && cpio -idvm)
    flex_file=obdevtools-flex-2.6.1-32023111015.el8.aarch64.rpm
    wget -q https://mirrors.aliyun.com/oceanbase/development-kit/el/8/aarch64/${flex_file} -O ${ENV_DEP_DIR}/${flex_file}
    rpm2cpio ${ENV_DEP_DIR}/${flex_file} | (cd ${ENV_DEP_DIR} && cpio -idvm)
    ;;
    **)
    echo "Unsupported os arch, please prepare the building environment in advance."
    ;;
esac
export PATH=${ENV_DEP_DIR}/usr/local/oceanbase/devtools/bin:$PATH
export FLEX=${ENV_DEP_DIR}/usr/local/oceanbase/devtools/bin/flex
CMAKE_COMMAND=${ENV_DEP_DIR}/usr/local/oceanbase/devtools/bin/cmake

# 4. install sqlite3
sqlite_file=sqlite-autoconf-3440200.tar.gz
if [ ! -f ${ENV_DEP_DIR}/${sqlite_file}.done ]; then
  cd ${ENV_DEP_DIR}
  wget -q --no-check-certificate https://sqlite.org/2023/${sqlite_file} -O ${sqlite_file} && \
  tar -zxvf ${sqlite_file} && cd sqlite-autoconf-3440200/ && \
  rm -rf /usr/bin/sqlite3 && \
  ./configure && make && make install && cd - && \
  ln -sf /usr/local/bin/sqlite3 /usr/bin/sqlite3 && \
  rm -rf rm llibsqlite3.so.0.8.6 && ln -sf /usr/local/lib/libsqlite3.so.0.8.6 /usr/lib64/libsqlite3.so.0.8.6 && \
  rm -rf rm /usr/lib64/libsqlite3.so && ln -sf /usr/local/lib/libsqlite3.so /usr/lib64/libsqlite3.so && \
  echo "/usr/local/lib" > /etc/ld.so.conf.d/sqlite3.conf && ldconfig
  if [ $? -ne 0 ]; then
    echo "Failed to install sqlite"
    exit 3
  fi
  touch ${ENV_DEP_DIR}/${sqlite_file}.done
fi


# 5. check installed deps
which make
which cmake
which gcc
which cc
which c++
which g++
which flex
which bison
which sqlite3
sqlite3_version=$(sqlite3 --version)
bison_version=$(bison --version)

echo "CMAKE_COMMAND=${CMAKE_COMMAND}, FLEX=$FLEX, SQLITE3_VERSION=${sqlite3_version}, BISON_VERSION=${bison_version}"
