#!/bin/bash

# standalone_upgrade.sh
function parse_index_file() {
  index_file=${1};
  max_timestamp=${2}
  max_current=''
  max_checkpoint=''
  curr_binlog=''
  curr_mapping=''
  curr_checkpoint=''
  while IFS=$'\t' read -r binlog index current before checkpoint pos ; do
    curr_binlog=${binlog}
    curr_mapping=${current}
    curr_checkpoint=${checkpoint}
    if [[ ${checkpoint} -ge ${max_timestamp} ]]; then
      max_current=${current}
      max_checkpoint=${checkpoint}
      break
    fi
  done < ${index_file}

  if [[ ${max_current} == '' ]]; then
    echo "!! Notice: Not matched binlog file with checkpoint greater than timestamp: ${max_timestamp}, and using latest binlog: ${curr_binlog}, current_mapping: ${curr_mapping}, current_checkpoint: ${curr_checkpoint}"
    xid=$(echo ${curr_mapping} | awk -F'=' '{print $1}')
    gtid=$(echo ${curr_mapping} | awk -F'=' '{print $2}')
    if [[ ${xid} == '' ]]; then
      echo "!!! Empty gtid mapping !!! "
      mapping="${xid}\t${gtid}\t${max_timestamp}"
    else
      echo '!!! No data coming, causing the last record is smaller than min_clog_timestamp!!!'
      gtid=$((gtid + 1))
      mapping="\t${gtid}\t${max_timestamp}"
    fi
  else
    echo "Matched binlog file: ${binlog}, max_current: ${max_current}, max_checkpoint: ${max_checkpoint}"
    xid=$(echo ${max_current} | awk -F'=' '{print $1}')
    gtid=$(echo ${max_current} | awk -F'=' '{print $2}')
    mapping="${xid}\t${gtid}\t${max_checkpoint}"
  fi
  echo "gtid mapping: ${mapping}"
}

function get_min_clog_timestamp() {
  cluster_url=${1}
  cluster_user=${2}
  cluster_password=${3}
  url_response=$(curl -Ls "${cluster_url}")
  port=$(echo "${url_response}" | jq -r '.Data.RsList[0].sql_port')
  host_str=$(echo "${url_response}" | jq -r '.Data.RsList[0].address')
  host=$(echo ${host_str} | awk -F':' '{print $1}')

  version=$(mysql -h${host} -P ${port} -u${cluster_user} -p${cluster_password} -A -e "show parameters like 'min_observer_version'" | awk 'NR>1 {print $7}' | head -n 1)
  major_version=${version:0:1}
  echo "version: ${version}, host: ${host}, port: ${port}"
  if [[ ${major_version} = "4" ]]; then
    min_clog_timestamp=$(mysql -h${host} -P ${port} -u${cluster_user} -p${cluster_password} -A -e "SELECT CEIL(MAX(BEGIN_SCN)/1000) AS START_TS_US FROM oceanbase.GV\$OB_LOG_STAT;" | awk 'NR>1')
    min_arch_clog_timestamp=$(mysql -h${host} -P ${port} -u${cluster_user} -p${cluster_password} -A -e "SELECT CEIL(MAX(START_SCN)/1000) as START_TS_US FROM oceanbase.DBA_OB_ARCHIVELOG;" | awk 'NR>1')
    echo "min_online_clog_timestamp: ${min_clog_timestamp}, min_arch_clog_timestamp: ${min_arch_clog_timestamp}"
    if [[ ${min_arch_clog_timestamp} != "NULL" ]]; then
      min_clog_timestamp=$(echo -e "${min_arch_clog_timestamp}\n${min_online_clog_timestamp}" | bc | sort -nr | head -n 1)
    fi
  else
    min_clog_timestamp=$(mysql -h${host} -P ${port} -u${cluster_user} -p${cluster_password} -A -sNe "SELECT svr_min_log_timestamp FROM oceanbase.__all_virtual_server_clog_stat WHERE zone_status='ACTIVE';" | head -n 1)
  fi
}

function generate_create_binlog_sql() {
  cluster=${1}
  tenant=${2}
  tenant_conf_file=${3}
  create_binlog_file=${5}

  cluster_url=$(jq -r ".OblogConfig.configs.cluster_url" ${tenant_conf_file})
  cluster_user=$(jq -r ".OblogConfig.configs.cluster_user" ${tenant_conf_file})
  cluster_password=$(jq -r ".OblogConfig.configs.cluster_password" ${tenant_conf_file})
  echo "cluster_url: ${cluster_url}, cluster_user: ${cluster_user}"

  index_file="/home/ds/oblogproxy/run/${cluster}/${tenant}/data/mysql-bin.index"
  first_timestamp=$(awk -F$'\t' 'NR=1 {print $5}' ${index_file} | head -n 1)
  echo "index_file: ${index_file}, first_timestamp: ${first_timestamp}"
  get_min_clog_timestamp ${cluster_url} ${cluster_user} ${cluster_password}
  max_timestamp=$(echo -e "${first_timestamp}\n${min_clog_timestamp}" | bc | sort -nr | head -n 1)
  echo "min_clog_timestamp: ${min_clog_timestamp}, max_timestamp: ${max_timestamp}"

  parse_index_file ${index_file} ${max_timestamp}
  if [[ ${mapping} == "-1" ]]; then
    create_sql="CREATE BINLOG IF NOT EXISTS FOR TENANT \`${cluster}\`.\`${tenant}\` FROM <todo> WITH CLUSTER URL \`${cluster_url}\`, INITIAL_TRX_XID \`todo\`,INITIAL_TRX_GTID_SEQ \`todo\`;"
    echo -e "FAIL # ${cluster} # ${tenant} # ${create_sql} # ${4}" | tee -a ${create_binlog_file}
    return 1
  else
    xid=$(echo -e ${mapping} | awk  -F$'\t' '{print $1}')
    gtid=$(echo -e ${mapping} | awk  -F$'\t' '{print $2}')
    start_timestamp=$(echo -e ${mapping} | awk  -F$'\t' '{print $3}')
    if [[  ${xid} == '' && ${gtid} == 0 ]]; then
      create_sql="CREATE BINLOG IF NOT EXISTS FOR TENANT \`${cluster}\`.\`${tenant}\` FROM ${start_timestamp} WITH CLUSTER URL \`${cluster_url}\`;"
    elif [[ ${xid} == '' && ${gtid} > 0 ]]; then
      create_sql="CREATE BINLOG IF NOT EXISTS FOR TENANT \`${cluster}\`.\`${tenant}\` FROM ${start_timestamp} WITH CLUSTER URL \`${cluster_url}\`, INITIAL_TRX_GTID_SEQ \`${gtid}\`;"
    else
      create_sql="CREATE BINLOG IF NOT EXISTS FOR TENANT \`${cluster}\`.\`${tenant}\` FROM ${start_timestamp} WITH CLUSTER URL \`${cluster_url}\`, INITIAL_TRX_XID \`${xid}\`,INITIAL_TRX_GTID_SEQ \`${gtid}\`;"
    fi
    echo -e "SUCCESS # ${cluster} # ${tenant} # ${create_sql} # ${4}" | tee -a ${create_binlog_file}
    return 0
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

function meet_create_condition() {
  uid=${1}
  cluster=${2}
  tenant=${3}

  case C"${granularity}" in
  Cuser)
    if [[ ${uid} == "${s_uid}" ]]; then
      return 0
    fi
    ;;
  Ccluster)
    if [[ ${cluster} == "${s_cluster}" ]]; then
      return 0
    fi
    ;;
  Ctenant)
    if [[ ${cluster} == "${s_cluster}" && ${tenant} == "${s_tenant}" ]]; then
      return 0
    fi
    ;;
  C*)
    echo "Unexpected granularity: ${granularity}"
    ;;
  esac
  return 1
}

function is_binlog_alive() {
  cluster=${1}
  tenant=${2}

  binlog_num=$(ps aux | grep ${cluster} | grep ${tenant} | grep binlog | wc -l)
  if [[ ${binlog_num} == 1 ]]; then
    return 0
  else
    echo "${cluster}.${tenant} may not alive, num: ${binlog_num}"
    return 1
  fi
}

function create_binlog() {
    gen_filename_prefix
    create_binlog_file="${prefix}_create_binlog.txt"
    rm -f ${create_binlog_file}

    not_found_tenant=()
    success_tenant=()
    failed_tenant=()
    while read -r uid cluster tenant config ; do
      meet_create_condition ${uid} ${cluster} ${tenant}

      if [[ $? -eq 0 ]]; then
        tenant_conf_file="/home/ds/oblogproxy/run/${cluster}/${tenant}/binlog_converter.conf"
        if [ ! -f "${tenant_conf_file}" ]; then
          not_found_tenant[${#not_found_tenant[*]}]=${cluster}.${tenant}
        else
          echo -e "================= cluster: ${cluster}, tenant: ${tenant}  =================="

          is_binlog_alive ${cluster} ${tenant}
          if [ $? -eq 0 ]; then
            generate_create_binlog_sql ${cluster} ${tenant} ${tenant_conf_file} ${config} ${create_binlog_file}
            if [ $? -eq 0 ]; then
              success_tenant[${#success_tenant[*]}]=${cluster}.${tenant}
            else
              failed_tenant[${#failed_tenant[*]}]=${cluster}.${tenant}
            fi
          else
             create_sql="CREATE BINLOG IF NOT EXISTS FOR TENANT \`${cluster}\`.\`${tenant}\` WITH CLUSTER URL \`${cluster_url}\`;"
             echo -e "FAILED # ${cluster} # ${tenant} # ${create_sql} # ${config}" | tee -a ${create_binlog_file}

            failed_tenant[${#failed_tenant[*]}]=${cluster}.${tenant}
          fi
          echo -e "\n"
        fi
      fi
    done < ${subscription_file}

    echo "************************** [Create binlog] Summary  **********************************"
    echo "生成 create binlog sql 成功的租户: ${success_tenant[*]}"
    echo "生成 create binlog sql 失败的租户: ${failed_tenant[*]}"
    echo "未找到工作目录的租户: ${not_found_tenant[*]}"
    echo "输出到文件: ${create_binlog_file}"
    echo -e "\n"
}

function print_min_dump_sql() {
  echo "*** 获取租户的最小订阅位点 ***"
  echo "Step 1: 在 sls 上执行下述 SQL: "
  echo "not dump_info:'null' | WITH t1 as (SELECT  cluster, tenant,  trace_id, max(dump_info) as curr_dump  from logproxy where dump_info like 'file:/home/ds%' group by cluster, tenant, trace_id limit 10000)  SELECT cluster,tenant, min(curr_dump) as min_dump from t1 group by cluster, tenant";
  echo "Step 2: 拷贝下载 URL, 执行下述语句并解压： "
  echo "wget -O 'dump.csv.gz'  '<URL>' "
  echo "gunzip dump.csv.gz"

  echo -e "\n"
  echo "*** 获取租户的下游订阅数量  ***"
  echo "Step 1: 在 sls 上执行下述 SQL: "
  echo "Step 2: 拷贝下载 URL, 执行下述语句并解压： "
  echo "wget -O 'dump_count.csv.gz' '<URL' "
  echo "gunzip dump_count.csv.gz"
  echo -e "\n"
}

function match_checkpoint() {
  cluster=${1}
  tenant=${2}
  d_line=${3}

  binlog_file=$(echo ${d_line} | grep -oP 'file:(\K[^,]+)')
  index_file="/home/ds/oblogproxy/run/${cluster}/${tenant}/data/mysql-bin.index"
  echo "binlog_file: ${binlog_file}"
  min_checkpoint=''
  while IFS=$'\t' read  -r binlog index current before checkpoint pos; do
    if [[ ${binlog_file} == ${binlog} ]]; then
       echo "Match min dump binlog file: ${binlog}, ${current}, ${checkpoint}"
       break
    else
       min_checkpoint=${checkpoint}
    fi
  done < ${index_file}

  if [[ ${min_checkpoint} == '' ]]; then
    echo "Not matched min dump binlog file: ${binlog_file}"
  fi
}

function get_min_dump_checkpoint() {
  gen_filename_prefix
  user_tenants_file="${prefix}_create_binlog.txt"
  checkpoint_file="${prefix}_min_dump_checkpoint.txt"
  rm -f ${checkpoint_file}

  success_tenant=()
  failed_tenant=()
  no_dump_tenants=()
  while read -r line ; do
    state=$(echo ${line} | awk -F' *# *'  '{print $1}')
    cluster=$(echo ${line} | awk -F' *# *' '{print $2}')
    tenant=$(echo ${line} | awk -F' *# *' '{print $3}')
    echo -e "=================  cluster: ${cluster}, tenant: ${tenant}  =================="

    min_checkpoint='0'
    while read -r d_line o1 o2; do
      d_cluster=$(echo "${d_line}" | awk -F',' '{print $1}')
      d_tenant=$(echo "${d_line}" | awk -F',' '{print $2}')

      if [[ ${cluster} == ${d_cluster} && ${tenant} == ${d_tenant} ]]; then
         echo "Found downstream subscription, min dump file: ${d_line}"
         match_checkpoint ${cluster} ${tenant} ${d_line}
      fi
    done < ${dump_info_file}

    if [[ ${min_checkpoint} == '' ]]; then
       echo "!!! Not found dump info, please handle it manually"
       failed_tenant[${#failed_tenant[*]}]=${cluster}.${tenant}
    elif [[ ${min_checkpoint} == '0' ]]; then
        echo "Tenant has no dump..."
        no_dump_tenants[${#no_dump_tenants[*]}]=${cluster}.${tenant}
    else
       success_tenant[${#success_tenant[*]}]=${cluster}.${tenant}
    fi
    echo "${cluster} # ${tenant} # ${min_checkpoint}" | tee -a ${checkpoint_file}
    echo -e "\n"
  done < ${user_tenants_file}


  echo "************************** [get min dump checkpoint] Summary  **********************************"
  echo "有下游订阅的租户: ${success_tenant[*]}"
  echo "有下游订阅但未匹配到位点租户，请手动填充: ${failed_tenant[*]}"
  echo "无下游订阅的租户: ${no_dump_tenants[*]}"
  echo "输出到文件: ${checkpoint_file}"
  echo -e "\n"
}

function check_gtid_consistency() {
  binlog_file=''
  gtid=0
  index_file="/home/ds/oblogproxy/run/${s_cluster}/${s_tenant}/data/mysql-bin.index"
  echo "${s_cluster}  ${s_tenant}  ${server_uuid}  ${s_gtid}"
  event_cmd=''
  while IFS=$'\t' read -r binlog index current before checkpoint pos ; do
    echo "${binlog} ${current}"
    if [[ ${gtid} -gt ${s_gtid} ]]; then
      if [[ ${binlog_file} == '' ]]; then
        echo "!!! Notice: No matched gtid"
      else
        event_cmd="/home/ds/switch/mysql/bin/mysqlbinlog  --base64-output=decode-rows --include-gtids '${server_uuid}:${s_gtid}' ${binlog_file} > 222.binlog"
      fi
      break
    else
      binlog_file=${binlog}
      gtid=$(echo ${current} | awk -F'=' '{print $2}')
    fi
  done < ${index_file}

  if [[ -z ${event_cmd} ]]; then
    event_cmd="/home/ds/switch/mysql/bin/mysqlbinlog  --base64-output=decode-rows --include-gtids '${server_uuid}:${s_gtid}' ${binlog_file} > 222.binlog"
  fi
  echo -e "${event_cmd}     \c"
  eval ${event_cmd}
  if [ $? -eq 0 ]; then
    echo "EXEC SUCCESS"
    echo ""
    grep "Table_map" 222.binlog
    echo ""
  else
    echo "EXEC FAILED"
  fi
}

mode="create"
dump_info_file="dump.csv"
s_uid=''
subscription_file="binlog_subscription.txt"
s_cluster=''
s_tenant=''
granularity=''  # user/cluster/tenant
usage() {
  echo "Usage: $0 -u <uid> -c <cluster> -t <tenant> -s <subscription file> -m <mode> -g <granularity> -d <dump info file> -S <server uuid> -G <gtid>"
  exit 0
}
while getopts "u:c:t:s:m:g:d:h:S:G:" o; do
  case "${o}" in
  s)
    subscription_file=${OPTARG}
    ;;
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
  d)
    dump_info_file=${OPTARG}
    ;;
  S)
    server_uuid=${OPTARG}
    ;;
  G)
    s_gtid=${OPTARG}
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
# 参数: uid(必须)、subscription_file(可选)
Ccreate)
  echo "Begin to generate create binlog SQL for ${granularity}: ${s_uid} ${s_cluster} ${s_tenant}"
  create_binlog
  ;;
Cgtid)
  check_gtid_consistency
  ;;
Cdump)
  print_min_dump_sql
  ;;
# 参数: uid(必须)、dump_info_file(可选)
Ccheckpoint)
  get_min_dump_checkpoint
  ;;
Cinit)
  yum install -y jq
  ;;
C*)
  echo "Usage: $0 -m {init|user_create|dump|checkpoint}"
  ;;
esac
