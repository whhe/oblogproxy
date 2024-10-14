#!/bin/bash

tenant_file="standby_tenant.txt"
tenant_create_sql_file="tenant_create_sql.txt"
rm -f ${tenant_create_sql_file}

while read -r cluster tenant ; do
    work_path="/home/ds/oblogproxy/run/${cluster}/${tenant}"
    tenant_conf_file="/home/ds/oblogproxy/run/${cluster}/${tenant}/binlog_converter.conf"
    if [ ! -f "${tenant_conf_file}" ]; then
      echo "Not found work path"
    else
      echo -e "=================  cluster: ${cluster}, tenant: ${tenant}  ==================\n"
      cluster_url=$(jq -r ".OblogConfig.configs.cluster_url" ${tenant_conf_file})

      index_file="/home/ds/oblogproxy/run/${cluster}/${tenant}/data/mysql-bin.index"
      current=$(cat ${index_file} | tail -n 1 | awk '{print $3}')
      xid=${current%=*}
      gtid=${current#*=}
      start_timestamp=$(cat ${index_file} | tail -n 1 | awk '{print $5}')
      if [[ xid != '' ]]; then
        create_sql="CREATE BINLOG IF NOT EXISTS FOR TENANT \`${cluster}\`.\`${tenant}\` FROM ${start_timestamp} WITH CLUSTER URL \`${cluster_url}\`, INITIAL_TRX_XID \`${xid}\`,INITIAL_TRX_GTID_SEQ \`${gtid}\`;"
        echo ${create_sql} | tee -a ${tenant_create_sql_file}
      else
        echo "No available gtid mapping"
      fi
    fi
done < ${tenant_file}

