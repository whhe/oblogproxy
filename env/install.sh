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
  deploy_path=/home/ds
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

if [ ! -d "${deploy_path}/oblogproxy" ]; then
  echo "Not exist oblogproxy path: ${deploy_path}/oblogproxy"
  exit -1
fi

set -x
mkdir ${deploy_path}/log
#Initialize some environment dependencies
yum install -y libtiff

# configs
cd ${deploy_path}/oblogproxy
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

if [ "$mode" == "oblogproxy" ]; then
  cp -rf env/supervisord.d/oblogproxy.ini /etc/supervisord.d/oblogproxy.ini
elif [ "$mode" == "binlog" ]; then
  cp -rf env/supervisord.d/binlog.ini /etc/supervisord.d/binlog.ini
else
  echo "UnSupported mode: ${mode}"
  exit -1
fi
systemctl enable supervisord
service supervisord start
