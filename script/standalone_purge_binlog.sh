#!/bin/bash

# standalone_purge_binlog.sh
usage() {
  echo "Usage: $0 -f <tenants that have been switched to cluster version> -t <sleep time(s)> -p <mysql port>"
  exit 0
}
switch_done_file="switch_done_file.txt"
sleep_time_s=600
ip=127.0.0.1
port=2983
while getopts "f:t:p:h" o; do
  case "${o}" in
  f)
    switch_done_file=${OPTARG}
    ;;
  t)
    sleep_time_s=${OPTARG}
    ;;
  p)
    port=${OPTARG}
    ;;
  h)
    usage
    ;;
  *)
    usage
    ;;
  esac
done
switch_done_file=$(readlink -f ${switch_done_file})

function purge_by_expire_seconds() {
  cluster=${1}
  tenant=${2}
  binlog_retention_h=${3}

  before_time=$(date -d "-${binlog_retention_h} hours" +"%Y-%m-%d %H:%M:%S")
  purged_seconds_sql="PURGE BINARY LOGS BEFORE '"${before_time}"' FOR TENANT \`"${cluster}"\`.\`"${tenant}"\`"

  echo "Begin exec sql: ${purged_seconds_sql}"
  exec_result=$(mysql -A -c -h ${ip} -P ${port} -e "${purged_seconds_sql}")
  if [ $? -eq 0 ]; then
    echo -n "Successfully "
  else
    echo -n "Failed to "
  fi
  echo "purge binlog before ${before_time}, exec sql result: ${exec_result}"
}

function purge_by_expire_size() {
  cluster=${1}
  tenant=${2}
  expire_size_bytes=$(expr ${3} \* 1024 \* 1024)

  data_dir="/home/ds/oblogproxy/run/${cluster}/${tenant}/data"
  cd ${data_dir}
  binlog_total_bytes=$(find . -type f -name 'mysql-bin.*[0-9][0-9][0-9][0-9][0-9][0-9]' -exec du -cb {} + | grep 'total$' | awk '{print $1}')
  if [[ ${binlog_total_bytes} -le ${expire_size_bytes} ]]; then
    echo "Total binlog size is less than expire_size_bytes: ${binlog_total_bytes} < ${expire_size_bytes}"
    return 0
  fi

  diff_bytes=$(expr ${binlog_total_bytes} - ${expire_size_bytes})
  binlog_num=$(expr ${diff_bytes} / 536870912)
  echo "expire_size_bytes: ${expire_size_bytes}, binlog_total_bytes: ${binlog_total_bytes}, diff_bytes: ${diff_bytes}, binlog_num: ${binlog_num}"
  if [[ ${binlog_num} -ge 1 ]]; then
    binlog_file=$(ls | grep 'mysql-bin.*[0-9][0-9][0-9][0-9][0-9][0-9]' | sort | head -n ${binlog_num} | tail -n 1)
    purged_size_sql="PURGE BINARY LOGS TO '"${binlog_file}"' FOR TENANT \`"${cluster}"\`.\`"${tenant}"\`"

    echo "Begin exec sql: ${purged_size_sql}"
    exec_result=$(mysql -A -c -h ${ip} -P ${port} -e "${purged_size_sql}")
    if [ $? -eq 0 ]; then
      echo -n "Successfully "
    else
      echo -n "Failed to "
    fi
    echo "purge binlog before ${binlog_file}, exec sql result: ${exec_result}"
  fi
}

function check_and_purge_binlog() {
  while read -r uid cluster tenant expire_config; do
    disk_capacity=$(echo ${expire_config} | jq -r ".diskCapacity")
    log_retention_h=$(echo ${expire_config} | jq -r ".logRetentionTime")

    echo -e "\n***** ${cluster}.${tenant}  expire_mb: ${disk_capacity}, expire_hour: ${log_retention_h} *****"
    data_path="/home/ds/oblogproxy/run/${cluster}/${tenant}/data"
    echo "Binlog file path: ${data_path}"
    if [[ -d "${data_path}" ]]; then
      purge_by_expire_size ${cluster} ${tenant} ${disk_capacity}
      purge_by_expire_seconds ${cluster} ${tenant} ${log_retention_h}
    else
      echo "Binlog file path does not exist: ${data_path}"
    fi
  done < ${switch_done_file}
}

echo "Current params, switch_done_file: ${switch_done_file}, sleep_time_s: ${sleep_time_s}, IP: ${ip}, mysql port: ${port}"
while [[ true ]]; do
  current_time=$(date +"%Y-%m-%d %H:%M:%S")
  echo "========== Begin to purge binlog at ${current_time} =========="

  check_and_purge_binlog

  echo "========== Finished purge binlog, and begin to sleep ${sleep_time_s} second =========="
  echo ""
  sleep ${sleep_time_s}
done