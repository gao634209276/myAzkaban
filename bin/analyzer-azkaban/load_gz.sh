#!/bin/bash
#获取配置文件通用方法
GetConf(){    
    section=$(echo $1 | cut -d '.' -f 1)    
    key=$(echo $1 | cut -d '.' -f 2)    
    sed -n "/\[$section\]/,/\[.*\]/{    
     /^\[.*\]/d    
     /^[ \t]*$/d    
     /^$/d    
     /^#.*$/d    
     s/^[ \t]*$key[ \t]*=[ \t]*\(.*\)[ \t]*/\1/p    
    }" ~/bin/analyzer/conf/sys_conf.ini    
}
#初始化路径配置
app_home=$(GetConf "local.app_home")
src_logs=$(GetConf "local.src_logs")
mkdir -p ${src_logs}
meta_logs=$(GetConf "local.meta_logs")
mkdir -p ${meta_logs}
host=$(GetConf "remote.host")
uname=$(GetConf "remote.uname")
remote_root=$(GetConf "remote.root")

if   [   $#   -lt 1     ]
then
    echo "useage: sh load_data.sh <date> <log_type> <run_type>"
else
hour=""
hosts_ip=""
sub_dir=""
if [ $1 = "hour_ago" ]
then
	hour=`date '-d 1 hour ago' +%Y%m%d%H`
elif [ $1 = "day_ago" ]
then
	hour=`date '-d 1 day ago' +%Y%m%d`
else
	hour=$1
fi

echo "hour:"${hour}
day=${hour:0:8}

stime=$(date +%s -d 'now')
root_dir="/disk2/backup_disk3/daily/${hour}/"
local_dir="${src_logs}/gz/${day}"
mkdir -p  ${local_dir}
#ad iservice_biz mob_interface sms tyrz client iservice_interface nginx favorite mob_biz push sms_sfts
#ad biz_intf biz_mob biz_sms biz_sms_sfts biz_web client fav login nginx push 
#biz_sms_sfts-sms_sfts

tpy="ad-ad biz_mob-mob_biz  biz_web-iservice_biz client-client fav-favorite login-tyrz push-push biz_sms-sms"
for str in ${tpy}
do
	OLD_IFS="$IFS" 
	IFS="-" 
	arr=($str) 
	IFS="$OLD_IFS"
	log_type=${arr[0]}
	src_type=${arr[1]} 
	dest="${src_logs}/${log_type}/${day}"
	mkdir -p ${dest}
	    echo  ${uname}@${host}:${root_dir}/*${src_type}*.log.gz
	    scp ${uname}@${host}:${root_dir}/*${src_type}*.log.gz ${dest} 
	gunzip ${dest}/*.log.gz
	sh ${app_home}/analyzer/etl.sh ${day} ${log_type} rerun &
done
etime=$(date +%s -d 'now')
echo "总耗时:(分钟)"$((($etime-$stime)/60))
fi
