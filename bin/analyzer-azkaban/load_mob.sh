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
    }" ~/bin/analyzer-azkaban/conf/sys_conf.ini    
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
if   [   $#   -lt 2     ]
then
    echo "usage: sh load_data.sh <date> <log_type> <run_type>"
else
hour=""
hosts_ip=""
sub_dir=""
log_type=$2
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
local_dir="${src_logs}/${log_type}/${day}"
mkdir -p  ${local_dir}
log_file="*${hour}*.log"

stime=$(date +%s -d 'now')
#根据日志类型,指定查找



if [ ${log_type} = "mob_biz" ]
then
        root_dir="/disk3/YH_DATA/DATA/service_new"
        sub_dir="service"
        hosts_ip="10.20.34.11 10.20.34.12 10.20.34.13 10.20.34.14 10.142.194.25 10.142.194.26 10.142.194.27 10.142.194.28 10.142.194.29 10.142.194.30 10.142.194.31 10.142.194.32"

        for IP in ${hosts_ip}
        do
          echo  ${uname}@${host}:${root_dir}/${sub_dir}/${IP}/info/*/${log_file} ${local_dir}
         scp ${uname}@${host}:${root_dir}/${sub_dir}/${IP}/info/*/${log_file} ${local_dir}
        done
fi
if [ ${log_type} = "mob_intf" ]
then
        root_dir="/disk3/YH_DATA/DATA/service_new"
        sub_dir="interface"
        hosts_ip="10.20.34.11 10.20.34.12 10.20.34.13 10.20.34.14 10.142.194.25 10.142.194.26 10.142.194.27 10.142.194.28 10.142.194.29 10.142.194.30 10.142.194.31 10.142.194.32"

        for IP in ${hosts_ip}
        do
          echo  ${uname}@${host}:${root_dir}/${sub_dir}/${IP}/interface/*/${log_file} ${local_dir}
         scp ${uname}@${host}:${root_dir}/${sub_dir}/${IP}/interface/*/${log_file} ${local_dir}
        done
fi



#统计文件信息
cd ${local_dir}
echo 'unzip files if zip'
for files in `ls *.*`
do
	if [ "${files##*.}" = "gz"  ]
	then
        	echo 'gunzip' $files
	        gunzip $files
        fi
done
f_count=`ls ${local_dir}/*${hour}*.log |wc -l`
rec_count=`cat ${local_dir}/*${hour}*.log |wc -l`
f_size=`du -hs`
#打印azkaban监控扫描信息
echo 'HIVE_TABLE_NAME:load-'$2'_'$1
echo 'Exported '$rec_count' records.'

run_type=''
rt=$3
if [[ $rt = "rerun" ]]
then
	run_type="手动执行"
else
	run_type="定时执行"
fi
etime=$(date +%s -d 'now')
echo "["${run_type}"] 总耗时:(秒)"$((($etime-$stime)))
echo "采集日志文件时间:[${hour}],采集类型:[${run_type}],总文件个数:[${f_count}],总记录数:[${rec_count}],总文件大小:["${f_size}"]" >> ${meta_logs}/${log_type}_${day}.meta

#$3='' 定时执行,$3=rerun 重新运行
#sh ${app_home}/analyzer/etl.sh ${hour} ${log_type} $3
fi
