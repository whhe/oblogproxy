#!/bin/bash

usage() {
  echo "Usage: $0 -m <mode, either deploy or upgrade> -f <deploy configure file> -a <auto install deps> -d <deploy path, default, /home/ds/oblogproxy> -u <sys user> -p <sys password>"
  echo -e "Example: \n sh deploy.sh -m deploy -f deploy.conf.json \n sh deploy.sh -m upgrade -d <deploy_path>"
  exit 0
}
auto_install_deps="true"
while getopts "m:f:a:d:u:p:h" o; do
  case "${o}" in
  m)
    mode=${OPTARG}
    ;;
  f)
    deploy_conf=${OPTARG}
    ;;
  a)
    auto_install_deps=${OPTARG}
    ;;
  d)
    deploy_path=${OPTARG}
    ;;
  u)
    sys_user=${OPTARG}
    ;;
  p)
    sys_password=${OPTARG}
    ;;
  h)
    usage
    ;;
  *)
    usage
    ;;
  esac
done

function log_detail() {
  DATE=`date "+%Y-%m-%d %H:%M:%S"`
  echo "${DATE} $(whoami) execute $0: $@" >> ${log_file}
}

function log_result() {
  echo -e "{\"res\": \"${1}\", \"msg\": \"${2}\", \"log\":\"${log_file}\"}" | jq .
}

function init_env() {
  # 1. jq
  if ! [ -x "$(command -v jq)" ]; then
    log_detail "jq is not installed, auto_install_deps: ${auto_install_deps}"
    if [[ ${auto_install_deps} == "true" ]]; then
      sudo yum install -y jq >> ${log_file}
    else
      echo -e "{\"res\": \"FAILED\", \"msg\": \"init jq env failed\", \"log\":\"${log_file}\"}"
      exit -1
    fi
  fi
  log_detail "JQ_VERSION: $(jq --version)"

  # 2. gcc
  sudo cp ${deploy_path}/deps/lib/libstdc++.so.6.0.28 /usr/lib64/ &&  cd /usr/lib64  &&  sudo ln -sf libstdc++.so.6.0.28 libstdc++.so.6
  if [[ $? -ne 0 ]]; then
    log_result "FAILED" "init gcc env failed"
    exit -1
  fi

  # 3. mysql
  if ! [ -x "$(command -v mysql)" ]; then
    log_detail "mysql is not installed, auto_install_deps: ${auto_install_deps}"
    if [[ ${auto_install_deps} == "true" ]]; then
      sudo yum install -y mysql >> ${log_file}
    else
      log_result "FAILED" "init mysql env failed"
      exit -1
    fi
  fi
  log_detail "MYSQL_VERSION: $(mysql --version)"

  # 4. diff
  if ! [ -x "$(command -v diff)" ]; then
    log_detail "diffutils is not installed, auto_install_deps: ${auto_install_deps}"
    if [[ ${auto_install_deps} == "true" ]]; then
      sudo yum install -y diffutils >> ${log_file}
    else
      log_result "FAILED" "init diff env failed"
      exit -1
    fi
  fi
  log_detail "DIFF_VERSION: $(diff --version)"
}

function init_db_schema() {
  deploy_conf=$1
  schema_sql="${deploy_path}/conf/schema.sql"

  log_detail "SCHEMA_SQL=${schema_sql}"
  create_database_sql="CREATE DATABASE IF NOT EXISTS ${database}"
  mysql -h${host} -P${port} -u${user} -p${password} -e "${create_database_sql}" >> ${log_file}
  if [[ $? -ne 0 ]]; then
    log_result "FAILED" "init metadb database ${database} failed"
    exit -1
  fi

  mysql -h${host} -P${port} -u${user} -p${password} -D ${database} < ${schema_sql} >> ${log_file}
  if [[ $? -ne 0 ]]; then
    log_result "FAILED" "init metadb schema failed"
    exit -1
  fi
}

function modify_conf_file() {
  rm -rf ${deploy_path}/deps/lib/libjvm.so

  conf="${deploy_path}/conf/conf.json"
  log_detail "CONF_FILE=${conf}"
  cp ${conf} "${deploy_path}/conf/conf.json.bak"

  sed -i 's/"binlog_mode"[ ]*:[ ]*false/"binlog_mode": true/' ${conf}
  sed -i 's/"database_ip"[ ]*:[ ]*"[0-9.]*"/"database_ip": "'"${host}"'"/' ${conf}
  sed -i 's/"database_port"[ ]*:[ ]*[0-9]*/"database_port": '"${port}"'/' ${conf}
  sed -i 's/"database_name"[ ]*:[ ]*".*"/"database_name": "'"${database}"'"/' ${conf}
  sed -i 's/"user"[ ]*:[ ]*.*"/"user": "'"${user}"'"/' ${conf}
  sed -i 's/"password"[ ]*:[ ]*.*"/"password": "'"${password}"'"/' ${conf}
  sed -i 's/"node_ip"[ ]*:[ ]*"[0-9.]*"/"node_ip": "'"${node_ip}"'"/' ${conf}

  sed -r -i 's/"auth_user"[ ]*:[ ]*true/"auth_user": false/' ${conf}
  sed -r -i 's/"read_timeout_us"[ ]*:[ ]*2000000/"read_timeout_us": 100000/' ${conf}


  if [ ! -d "${deploy_path}/run" ]; then
    mkdir ${deploy_path}/run
  fi
}

function start_binlog() {
  if [[ ${supervise_start} == "true" ]]; then
    cd ${deploy_path}/env
    sh install.sh ${sys_user} ${sys_password} binlog ${deploy_path} >> ${log_file}
  else
    cd ${deploy_path}
    if [[ ! -z ${sys_user} &&  ! -z ${sys_password} ]]; then
      sh run.sh config_sys ${sys_user} ${sys_password} false >> ${log_file}
    fi
    sh run.sh start >> ${log_file}
  fi
}

function verify_params() {
  host=$(jq -r ".host" ${deploy_conf})
  port=$(jq -r ".port" ${deploy_conf})
  user=$(jq -r ".user" ${deploy_conf})
  password=$(jq -r ".password" ${deploy_conf})
  database=$(jq -r ".database" ${deploy_conf})
  if [[ -z "${database}" ]]; then
    database="binlog_cluster"
  fi

  node_ip=$(jq -r ".node_ip" ${deploy_conf})
  if [[ -z "${node_ip}" ]]; then
    echo "Given 'node_ip' is empty, try to use 'hostname -i' output $(hostname -i) as 'node_ip'"
    node_ip=$(hostname -i)
  elif [ "${node_ip}" != "$(hostname -i)" ]; then
    echo "Given 'node_ip': '${node_ip}' is different with output of 'hostname -i': '$(hostname -i)'"
    read -p "Will you continue to use '${node_ip}' ? [Y/n]" choice
    if [[ "$choice" == [Nn] ]]; then
      echo "Please modify ${deploy_conf} and try again"
      exit 1
    fi
  fi

  # 'hostname -i' should also be checked to avoid multiple addresses
  if [[ "${node_ip}" =~ ^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$ ]]; then
    log_detail "Given node_ip: ${node_ip}"
  else
    log_result "FAILED" "Invalid node_ip: ${node_ip}"
    exit -1
  fi

  log_detail "METADB_INFO = host: ${host}, database: ${database}, port: ${port}, user: ${user}, node_ip: ${node_ip}"
  if [[ -z "${host}" || -z "${port}" || -z "${database}" || -z "${user}" || -z "${password}" || -z "${node_ip}" ]]; then
    log_result "FAILED" "Unexpected metadb parameters, host: ${host}, port: ${port}, database: ${database}, user: ${user}, node_ip: ${node_ip}"
    exit -1
  fi

  sys_user=$(jq -r ".sys_user" ${deploy_conf})
  sys_password=$(jq -r ".sys_password" ${deploy_conf})
  if [[ -z "${sys_user}"  || -z "${sys_password}" ]]; then
    log_detail "!! Warning: Unexpected user parameters, sys user: ${sys_user}, may cause startup failed"
  fi

  supervise_start=$(jq -r ".supervise_start" ${deploy_conf})
  if [[ -z "${supervise_start}" ]]; then
    supervise_start="false"
  fi
  init_schema=$(jq -r ".init_schema" ${deploy_conf})
  if [[ -z "${init_schema}" ]]; then
    init_schema="true"
  fi
}

function check_binlog_status() {
  cd ${deploy_path}
  status=$(sh run.sh status | tail -n 1)
  log_detail "BINLOG_STATUS=${status}"
  if [[ ${status} == "status : 1" ]]; then
    log_result "SUCCESS"
  else
    log_result "FAILED" "start binlog failed"
  fi
}

function deploy_binlog() {
  deploy_path=$(dirname $(cd $(dirname $0);pwd))
  if [ ! -d "${deploy_path}/log" ]; then
    mkdir ${deploy_path}/log
  fi
  log_file="${deploy_path}/log/${mode}.log"
  if [[ -f ${log_file} ]]; then
    rm -f ${log_file}
  fi
  log_detail "DEPLOY_PATH=${deploy_path}"

  if [ -z ${deploy_conf} ]; then
    deploy_conf="${deploy_path}/env/deploy.conf.json"
  else
    deploy_conf=$(readlink -f ${deploy_conf})
  fi
  log_detail "DEPLOY_CONF_FILE=${deploy_conf}"

  if [ ! -f "${deploy_conf}" ]; then
    log_result "FAILED" "The deploy conf file does not exist"
    exit -1
  fi

  init_env
  log_detail "*** 1. Init machine env...      OK"
  verify_params
  log_detail "*** 2. Verify parameters from [${deploy_conf}]...      OK"
  if [[ ${init_schema} == "true" ]]; then
    init_db_schema
    log_detail "*** 3. Init metadb...     OK"
  else
    log_detail "*** 3. Skip init metadb...     OK"
  fi
  modify_conf_file
  log_detail "*** 3. Modify binlog conf...     OK"
  start_binlog
  log_detail "*** 4. Start binlog...         OK"

  check_binlog_status
}

function upgrade_conf() {
  old_conf="${deploy_path}/conf/conf.json"
  new_conf="${upgrade_path}/oblogproxy/conf/conf.json"

  old_conf_filtered="${deploy_path}/conf/conf.json.filtered"
  merged_conf="${deploy_path}/conf/conf.json.merged"
  jq -s '(.[1] | keys_unsorted) as $keys | .[0] | with_entries(select(.key as $k | $keys | index($k)))' ${old_conf} ${new_conf} > ${old_conf_filtered}
  jq -s ".[0] * .[1]" ${new_conf} ${old_conf_filtered} > ${merged_conf}

  log_detail "DIFF ${old_conf} ${merged_conf}"
  diff -Z ${old_conf} ${merged_conf} >> ${log_file}

  mv ${old_conf} "${deploy_path}/conf/conf.json.bak" &&  mv ${merged_conf} ${old_conf} && rm ${old_conf_filtered}
  if [[ $? -ne 0 ]]; then
    log_result "FAILED" "upgrade conf failed"
    exit -1
  fi
}

function upgrade_schema() {
  conf_file="${deploy_path}/conf/conf.json"
  host=$(cat ${conf_file} | jq -r ".database_ip")
  port=$(cat ${conf_file} | jq -r ".database_port")
  database=$(cat ${conf_file} | jq -r ".database_name")
  user=$(cat ${conf_file} | jq -r ".user")
  password=$(cat ${conf_file} | jq -r ".password")

  schema_sql="${deploy_path}/conf/schema.sql"
  log_detail "DIFF ${schema_sql}  ${upgrade_path}/oblogproxy/conf/schema.sql"
  diff -Z  ${schema_sql}  ${upgrade_path}/oblogproxy/conf/schema.sql >> ${log_file}
  mv ${schema_sql} "${deploy_path}/conf/schema.sql.bak.$(date '+%Y%m%d_%H%M%S')" && cp "${upgrade_path}/oblogproxy/conf/schema.sql" ${schema_sql}

  log_detail "SCHEMA_SQL=${schema_sql}, CONF_FILE=${conf_file}, host: ${host}, port: ${port}, database: ${database}, user: ${user}"

  mysql -h${host} -P${port} -u${user} -p${password} -D ${database} < ${schema_sql} >> ${log_file}
  if [[ $? -ne 0 ]]; then
    log_result "FAILED" "upgrade metadb failed"
    exit -1
  fi
}

function upgrade_obcdc() {
  for obcdc_dir in $(ls ${deploy_path}/obcdc/); do
    obcdc_access_dir="${deploy_path}/obcdc/${obcdc_dir}"
    if [[ -d ${obcdc_access_dir} ]]; then
      obcdc_access_bak="${upgrade_path}/${obcdc_dir}-bak"
      if [[ -d ${obcdc_access_bak} ]]; then
        log_detail "rm -rf ${obcdc_access_bak}"
        rm -rf ${obcdc_access_bak}
      fi
      log_detail "mv ${obcdc_access_dir} ${obcdc_access_bak}"
      mv "${obcdc_access_dir}" "${obcdc_access_bak}"
    fi
  done

  log_detail "cp -R ${upgrade_path}/oblogproxy/obcdc/ ${deploy_path}/"
  cp -R ${upgrade_path}/oblogproxy/obcdc/ ${deploy_path}/
}

function upgrade_bin() {
  if [[ -f ${upgrade_path}/logproxy || -f  ${upgrade_path}/binlog_instance  || -f ${upgrade_path}/oblogreader ]]; then
    rm -f ${upgrade_path}/logproxy
    rm -f ${upgrade_path}/binlog_instance
    rm -f ${upgrade_path}/oblogreader
  fi

  mv ${deploy_path}/bin/logproxy ${upgrade_path}/logproxy &&
  mv ${deploy_path}/bin/binlog_instance ${upgrade_path}/binlog_instance &&
  mv ${deploy_path}/bin/oblogreader ${upgrade_path}/oblogreader &&
  cp ${upgrade_path}/oblogproxy/bin/logproxy ${deploy_path}/bin/logproxy &&
  cp ${upgrade_path}/oblogproxy/bin/binlog_instance ${deploy_path}/bin/binlog_instance &&
  cp ${upgrade_path}/oblogproxy/bin/oblogreader ${deploy_path}/bin/oblogreader

  if [[ $? -ne 0 ]]; then
    log_result "FAILED" "upgrade binary failed"
    exit -1
  fi
}

function upgrade_binlog() {
  if [[ -z ${deploy_path} ]]; then
    deploy_path=/home/ds/oblogproxy
  fi
  if [ ! -d "${deploy_path}/log" ]; then
    mkdir ${deploy_path}/log
  fi
  log_file="${deploy_path}/log/${mode}.log"
  if [[ -f ${log_file} ]]; then
    rm -f ${log_file}
  fi

  upgrade_path=$(dirname $(dirname $(cd $(dirname $0);pwd)))
  log_detail "UPGRADE_PATH=${upgrade_path}, DEPLOY_PATH=${deploy_path}"

  upgrade_conf
  log_detail "*** 1. Upgrade conf...      OK"
  upgrade_schema
  log_detail "*** 2. Upgrade metadb schema...      OK"
  upgrade_obcdc
  log_detail "*** 3. Upgrade OBCDC version...      OK"
  upgrade_bin
  log_detail "*** 4. Upgrade binary version...     OK"

  cp ${upgrade_path}/oblogproxy/env/deploy.sh ${deploy_path}/env/deploy.sh
  cp ${upgrade_path}/oblogproxy/run.sh ${deploy_path}/run.sh

  cd ${deploy_path}
  if [[ ! -z "${sys_user}" ]]; then
    if [[ ! -z "${sys_password}" ]]; then
        log_detail "Update sys ${sys_user}"
        sh run.sh config_sys ${sys_user} ${sys_password} false >> ${log_file}
    fi
  fi
  sh run.sh start >> ${log_file}

  check_binlog_status
}

case C"${mode}" in
Cdeploy)
  deploy_binlog
  ;;
Cupgrade)
  upgrade_binlog
  ;;
C*)
  usage
  ;;
esac

