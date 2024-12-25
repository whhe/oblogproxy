sys_user=$1
sys_password=$2
mode=$3
deploy_path=$4
if [ -z "${sys_user}" ] || [ -z "${sys_password}" ]; then
  echo "No input sys user or sys password"
  exit -1
fi

if [ -z "${mode}" ]; then
  mode="oblogproxy"
fi

if [ -z "${deploy_path}" ]; then
  deploy_path=/home/ds/oblogproxy
fi
#data_disk=/dev/vdb1

if [ ! -d "${deploy_path}" ]; then
  echo "Not exist deploy_path: ${deploy_path}"
  exit -1
fi

# cloud disk
#disk=$(df -h | grep "${data_disk}" | grep ${deploy_path})
#if [ -z "${disk}" ]; then
#  echo "No data disk: ${data_disk} exist for deploy_path: ${deploy_path}"
#  exit -1
#fi

set -x
if [ "${deploy_path}" == "/home/ds/oblogproxy" ]; then
  mkdir -p /home/ds/log
else
  mkdir -p ${deploy_path}/log
fi

#Initialize some environment dependencies
yum install -y libtiff

# configs
cd ${deploy_path}
if [[ ! -z ${sys_user} &&  ! -z ${sys_password} ]]; then
  sh run.sh config_sys ${sys_user} ${sys_password}
fi

if [ "$mode" == "oblogproxy" ]; then
  sed -r -i 's/"auth_user"[ ]*:[ ]*true/"auth_user": false/' ./conf/conf.json
elif [ "$mode" == "binlog" ]; then
  sed -r -i 's/"auth_user"[ ]*:[ ]*true/"auth_user": false/' ./conf/conf.json
  sed -r -i 's/"binlog_mode"[ ]*:[ ]*false/"binlog_mode": true/' ./conf/conf.json
  sed -r -i 's/"read_timeout_us"[ ]*:[ ]*2000000/"read_timeout_us": 100000/' ./conf/conf.json
else
  echo "UnSupported mode: ${mode}"
  exit -1
fi

# supervisord && start
yum install -y supervisor
cp -rf env/supervisord.conf /etc/supervisord.conf
if [ "${deploy_path}" != "/home/ds/oblogproxy" ]; then
  echo "Use customize deploy_path: ${deploy_path} replace /etc/supervisord.conf"
  sed -i "s|/home/ds|${deploy_path}|g" /etc/supervisord.conf
fi

if [ "$mode" == "oblogproxy" ]; then
  cp -rf env/supervisord.d/oblogproxy.ini /etc/supervisord.d/oblogproxy.ini
  if [ "${deploy_path}" != "/home/ds/oblogproxy" ]; then
    sed -i "s|/home/ds/oblogproxy|${deploy_path}|g" /etc/supervisord.d/oblogproxy.ini
    sed -i "s|/home/ds/log|${deploy_path}/log|g" /etc/supervisord.d/oblogproxy.ini
    echo "Use customize deploy_path : ${deploy_path} replace /etc/supervisord.d/oblogproxy.ini"
  fi
elif [ "$mode" == "binlog" ]; then
  cp -rf env/supervisord.d/binlog.ini /etc/supervisord.d/binlog.ini
  if [ "${deploy_path}" != "/home/ds/oblogproxy" ]; then
    sed -i "s|/home/ds/oblogproxy|${deploy_path}|g" /etc/supervisord.d/binlog.ini
    sed -i "s|/home/ds/log|${deploy_path}/log|g" /etc/supervisord.d/binlog.ini
    echo "Use customize deploy_path: ${deploy_path} replace /etc/supervisord.d/binlog.ini"
  fi
else
  echo "UnSupported mode: ${mode}"
  exit -1
fi
systemctl enable supervisord
service supervisord start
