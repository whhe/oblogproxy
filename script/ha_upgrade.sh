#!/bin/bash
# ha_upgrade.sh

port=2983
max_delay_threshold=180000
instance_num=1

function exec_sql() {
  sql=${1}
  echo -e "EXEC SQL: ${sql} ...         \c"
  mysql -h127.0.0.1 -P${port} -e "${sql}" > /dev/null 2> error.log
  if [ $? -ne 0 ]; then
    err_msg=$(cat error.log)
    echo "FAILED, error： ${err_msg}"
    return 1
  else
    echo "SUCCESS"
    return 0
  fi
}

function query_running_status() {
   cluster=${1}
   tenant=${2}

   sleep 3

   show_sql="SHOW BINLOG INSTANCES FOR '${cluster}'.'${tenant}'"
   echo -e "EXEC SQL: ${show_sql} ..."
   running_num=0;
   mkfifo myfifo
   mysql -h127.0.0.1 -P${port} -sNe "${show_sql}" > myfifo &
   while read -r line; do
       echo "$line"
       state=$(echo ${line} | awk '{print $7}')
       if [[ ${state} == "Running" ]]; then
         running_num=$((running_num+1))
       fi
   done < myfifo
   rm myfifo

   if [ ${running_num} == ${instance_num} ]; then
       echo "Total of [${instance_num}] binlog instances are running"
       return 0
   else
     echo "!!! Nof running instances is less than ${instance_num}, num: ${running_num}"
     return 1
   fi
}

function verify_expire_variables_for_instance() {
  instance=${1}
  logs_seconds=${2}
  logs_size=${3}

  expire_seconds_sql="SHOW VARIABLES LIKE 'BINLOG_EXPIRE_LOGS_SECONDS' for instance '${instance}'"
  echo "${expire_seconds_sql}"
  log_seconds_value=$(mysql -h127.0.0.1 -P${port} -sNe "${expire_seconds_sql}" | awk '{print $2}')
  if [[ ${logs_seconds} != ${log_seconds_value} ]]; then
    echo "${instance}: BINLOG_EXPIRE_LOGS_SECONDS are inconsistent: ${logs_seconds} != ${log_seconds_value}"
    return 1
  fi

  expire_log_size_sql="SHOW VARIABLES LIKE 'BINLOG_EXPIRE_LOGS_SIZE' for instance '${instance}'"
  echo "${expire_log_size_sql}"
  log_size_value=$(mysql -h127.0.0.1 -P${port} -sNe "${expire_log_size_sql}" | awk '{print $2}')
  if [[ ${logs_size} != ${log_size_value} ]]; then
    echo "${instance}: BINLOG_EXPIRE_LOGS_SIZE are inconsistent: ${logs_size} != ${log_size_value}"
    return 1
  fi

  return 0
}

function verify_expire_variables() {
   cluster=${1}
   tenant=${2}
   logs_seconds=${3}
   logs_size=${4}

   sleep 1

   show_sql="SHOW BINLOG INSTANCES FOR '${cluster}'.'${tenant}'"
   echo -e "EXEC SQL: ${show_sql} ..."
   failed_num=0;
   mkfifo myfifo
   mysql -h127.0.0.1 -P${port} -sNe "${show_sql}" > myfifo &
   while read -r line; do
       echo "$line"

       instance=$(echo ${line} | awk '{print $1}')
       verify_expire_variables_for_instance ${instance} ${logs_seconds} ${logs_size}
       if [ $? -ne 0 ]; then
         failed_num=$((failed_num+1))
       fi
       echo "${cluster}  ${tenant}  ${instance}"
       echo "sh ha_upgrade.sh -m gtid -c ${cluster} -t ${tenant}"
   done < myfifo
   rm myfifo

   if [[ ${failed_num} -eq 0 ]]; then
       echo "${cluster}.${tenant}: log expiration parameters verify passed"
       return 0
   else
       echo "${cluster}.${tenant}: log expiration parameters verify failed!!!"
       return 1
   fi
}

function alter_binlog() {
    cluster=${1}
    tenant=${2}
    config=${3}

    disk_capacity=$(echo ${config} | jq -r ".diskCapacity")
    logRetentionTime=$(echo ${config} | jq -r ".logRetentionTime")
    logs_size=$((disk_capacity * 1024 * 1024))
    logs_seconds=$((logRetentionTime * 60 * 60))
    alter_sql="ALTER BINLOG \`${cluster}\`.\`${tenant}\` SET BINLOG_EXPIRE_LOGS_SECONDS=${logs_seconds}, BINLOG_EXPIRE_LOGS_SIZE=${logs_size}"
    exec_sql "${alter_sql}"

    if [[ $? -eq 0 ]]; then
      echo "*** 4. verify expire variables ***"
      verify_expire_variables ${cluster} ${tenant} ${logs_seconds} ${logs_size}
      return $?
    else
      echo "Alter binlog expire variables failed!!"
      return 1
    fi
}

function gen_filename_prefix() {
  case C"${granularity}" in
  Cuser)
    prefix="${s_uid}"
    ;;
  Ccluster)
    prefix="${s_cluster}"
    ;;
  Ctenant)
    prefix="${s_cluster}_${s_tenant}"
    ;;
  C*)
    echo "Unexpected granularity: ${granularity}"
    ;;
  esac
}

function exec_create_binlog() {
    gen_filename_prefix
    create_sql_file="${prefix}_create_binlog.txt"
    exec_running_tenants=()
    exec_failed_tenants=()

    while read -r line ; do
      state=$(echo ${line} | awk -F' *# *'  '{print $1}')
      cluster=$(echo ${line} | awk -F' *# *' '{print $2}')
      tenant=$(echo ${line} | awk -F' *# *' '{print $3}')
      sql=$(echo ${line} | awk -F' *# *' '{print $4}')
      config=$(echo ${line} | awk -F' *# *' '{print $5}')
      is_success=1
      if [[ ${state} == "SUCCESS" ]]; then
        echo -e "=================  cluster: ${cluster}, tenant: ${tenant}  =================="
        echo "*** 1. create binlog ***"
        exec_sql "${sql}"

        if [ $? -eq 0 ]; then
            echo "*** 2. show  binlog ***"
            query_running_status ${cluster} ${tenant}

            if [ $? -eq 0 ]; then
               echo "*** 3. alter  binlog ***"
               alter_binlog ${cluster} ${tenant} ${config}

               if [[ $? -eq 0 ]]; then
                 is_success=0
               fi
            fi
        fi
        echo -e "\n"
      fi

      if [[ ${is_success} -eq 0 ]]; then
        exec_running_tenants[${#exec_running_tenants[*]}]=${cluster}.${tenant}
      else
        exec_failed_tenants[${#exec_failed_tenants[*]}]=${cluster}.${tenant}
      fi
    done < ${create_sql_file}

    echo "**************************  [exec create binlog] Summary  **********************************"
    echo "开通 binlog 成功的租户: ${exec_running_tenants[*]}"
    echo "开通 binlog 失败的租户: ${exec_failed_tenants[*]}"
    echo -e "\n"
}

function check_min_dump_checkpoint() {
   instance=${1}
   checkpoint=${2}

   echo "${instance} | ${checkpoint}"
   if [[ ${checkpoint} == 0 ]]; then
      echo "${instance}: no downstream subscription"
      return 0
   elif [[ ${checkpoint} == '' ]]; then
      echo "${instance}: empty checkpoint, please fill manually!!"
      return 1
   else
      index_file="/home/ds/oblogproxy/run/${instance}/data/mysql-bin.index"
      min_checkpoint=$(cat ${index_file} | head -n 1 | awk -F$'\t' '{print $5}')
      max_checkpoint=$(cat ${index_file} | tail -n 1 | awk -F$'\t' '{print $5}')
      if [[ ${min_checkpoint} == ${max_checkpoint} ]]; then
        echo "!!! Only 1 binlog file, please manually evaluate whether it meets the min dump checkpoint: ${checkpoint}"
        return 1
      else
        m_checkpoint=$((${checkpoint} / 1000000))
        if [[ ${checkpoint} -ge ${min_checkpoint} ]] && [[ ${m_checkpoint} -le ${max_checkpoint} ]]; then
          echo "${instance}: meet min dump checkpoint, ${min_checkpoint} <= ${m_checkpoint} <= ${max_checkpoint}"
          return 0
        else
          echo "${instance}: does not meet min dump checkpoint, ${min_checkpoint} <= ${m_checkpoint} <= ${max_checkpoint}"
          return 1
        fi
      fi
  fi
}

function check_convert_checkpoint() {
   cluster=${1}
   tenant=${2}
   checkpoint=${3}

   show_sql="SHOW BINLOG INSTANCES FOR '${cluster}'.'${tenant}'"
   echo -e "EXEC SQL: ${show_sql} ..."
   failed_num=0;
   mkfifo myfifo
   mysql -h127.0.0.1 -P${port} -sNe "${show_sql}" > myfifo &
   while read -r line; do
       echo "$line"
       instance=$(echo ${line} | awk '{print $1}')
       # maybe bc_running not in 11th field
       bc_running=$(echo ${line} | awk '{print $11}')
       delay=$(echo ${line} | awk '{print $12}')
       echo -e "1. check convert delay...      \c"
       if [[ ${bc_running} == "No" || ${delay} -gt ${max_delay_threshold} ]]; then
         echo "FAIL,  ${bc_running}: ${delay} > ${max_delay_threshold}"
         failed_num=$((failed_num+1))
       else
         echo "PASS"
         echo "2. check min dump checkpoint...    "
         check_min_dump_checkpoint ${instance} ${checkpoint}
         if [ $? -eq 1 ]; then
            failed_num=$((failed_num+1))
         fi
       fi
   done < myfifo
   rm myfifo

   if [ ${failed_num} == 0 ]; then
       echo "Total of [2] binlog instances meet checkpoint requirements"
       return 0
   else
     echo "!!! Nof instances that do not meet checkpoint requirements: ${failed_num}"
     return 1
   fi
}

function check_switch_requirement() {
    pass_tenants=()
    failed_tenants=()

    gen_filename_prefix
    tenant_min_checkpoint_file="${prefix}_min_dump_checkpoint.txt"
    while read -r line ; do
      cluster=$(echo ${line} | awk -F' *# *' '{print $1}')
      tenant=$(echo ${line} | awk -F' *# *' '{print $2}')
      checkpoint=$(echo ${line} | awk -F' *# *' '{print $3}')
      echo -e "=================  cluster: ${cluster}, tenant: ${tenant}  =================="
      check_convert_checkpoint ${cluster} ${tenant} ${checkpoint}

      if [ $? -eq 0 ]; then
        pass_tenants[${#pass_tenants[*]}]=${cluster}.${tenant}
      else
        failed_tenants[${#failed_tenants[*]}]=${cluster}.${tenant}
      fi
      echo -e "\n"
    done < ${tenant_min_checkpoint_file}

    echo "**************************  [check switch condition] Summary  **********************************"
    echo "满足切换 SLB 条件的租户: ${pass_tenants[*]}"
    echo "不满足切换 SLB 条件的租户: ${failed_tenants[*]}"
    echo -e "\n"
}

function print_dump_count_sql() {
    echo -e "\n"
    echo "*** 获取租户的下游订阅数量  ***"
    echo "Step 1: 在 sls 上执行下述 SQL: "
    echo "Step 2: 拷贝下载 URL, 执行下述语句并解压： "
    echo "wget -O 'dump_count.csv.gz' '<URL' "
    echo "gunzip dump_count.csv.gz"
    echo -e "\n"
}

function check_gtid_consistency() {
   show_sql="SHOW BINLOG INSTANCES FOR '${s_cluster}'.'${s_tenant}'"
   echo -e "EXEC SQL: ${show_sql} ..."
   mkfifo myfifo
   mysql -h127.0.0.1 -P${port} -sNe "${show_sql}" > myfifo &
   while read -r line; do
     echo "${line}"

     instance=$(echo ${line} | awk '{print $1}')
     server_uuid_sql="SHOW VARIABLES LIKE 'MASTER_SERVER_UUID' for instance '${instance}'"
     server_uuid=$(mysql -h127.0.0.1 -P${port} -sNe "${server_uuid_sql}" | awk '{print $2}')

     echo "$(cat /home/ds/oblogproxy/run/${instance}/data/mysql-bin.index | tail -n 1)"
     recent=$(cat /home/ds/oblogproxy/run/${instance}/data/mysql-bin.index | tail -n 1 | awk -F'\t' '{print $1, "|", $3}')
     binlog_file=$(echo ${recent} | awk -F'|' '{print $1}')
     gtid=$(echo ${recent} | awk -F'|' '{print $2}' | awk -F'=' '{print $2}')

     binlog="/home/ds/oblogproxy/run/${instance}/${binlog_file}"

     echo "${s_cluster}  ${s_tenant} ${instance} ${server_uuid} ${gtid} ${binlog}"
     event_cmd="/home/ds/switch/mysql/bin/mysqlbinlog  --base64-output=decode-rows --include-gtids '${server_uuid}:${gtid}' ${binlog} > 111.binlog"
     echo -e "${event_cmd}     \c"
     eval ${event_cmd}
     if [ $? -eq 0 ]; then
        echo "EXEC SUCCESS"
        echo ""
        grep "Table_map" 111.binlog
     else
        echo "EXEC FAILED"
     fi
     echo ""
     echo "sh standalone_upgrade.sh -m gtid -c ${s_cluster} -t ${s_tenant} -S ${server_uuid} -G ${gtid}"
   done < myfifo
   rm myfifo
}

mode="exec"
granularity=''
usage() {
  echo "Usage: $0 -m <mode>-g <granularity> -u <uid> -c <cluster> -t <tenant> -p <port> -n <instance_num>"
  exit 0
}
while getopts "u:c:t:m:g:h" o; do
  case "${o}" in
  u)
    s_uid=${OPTARG}
    ;;
  c)
    s_cluster=${OPTARG}
    ;;
  t)
    s_tenant=${OPTARG}
    ;;
  m)
    mode=${OPTARG}
    ;;
  g)
    granularity=${OPTARG}
    ;;
  p)
    port=${OPTARG}
    ;;
  n)
    instance_num=${OPTARG}
    ;;
  h)
    usage
    ;;
  *)
    usage
    ;;
  esac
done


case C"${mode}" in
Cexec)
  exec_create_binlog
  ;;
Cgtid)
  check_gtid_consistency
  ;;
Ccheck)
  check_switch_requirement
  ;;
Cdump)
  print_dump_count_sql
  ;;
Cinit)
  yum install -y jq
  ;;
C*)
  echo "Usage: $0 {init|exec|check}"
  ;;
esac

