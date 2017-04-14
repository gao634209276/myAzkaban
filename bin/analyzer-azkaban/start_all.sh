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
     /^[ \t]*$key[ \t]*=[ \t]*\(.*\)[ \t]*/\1/p    
    }" ~/bin/analyzer-azkaban/conf/sys_conf.ini  
}

#初始化路径配置
app_home=$(GetConf "local.app_home")
anal_dir=$(GetConf "local.anal_dir")
HADOOP_HOME=$(GetConf "local.hadoop_home")
HIVE_HOME=$(GetConf "local.hive_home")
export HADOOP_HOME HIVE_HOME
hive_warehouse=$(GetConf "hdfs.dw_home")
log_path=~/bin/logs

#传入参数
query_time=$1

if [ ${query_time} = "hour_ago" ]
then
	query_time=`date '-d 1 hour ago' +%Y%m%d%H`
elif [ ${query_time} = "day_ago" ]
then
	query_time=`date '-d 1 day ago' +%Y%m%d`
else
	query_time=$1
fi

len=`expr length ${query_time}` 
len=`echo ${query_time}|awk '{print length($0)}'` 

if [ ${len} -gt 8 ]; then
	day=${query_time:0:8}
else
	day=${query_time}
fi
echo ${query_time} ${day}

log_type=$2
hql=$3
tabs=$4
rerun=$5
if   [   $#   -lt 4     ]
then
    echo "usage: sh start_all.sh <query_time> <log_type split by ','> <hql> < hive_table_name split by ',' > <rerun>"
else

#抽取数据并执行ETL过程
echo "####执行数据抽及ETL主程序,执行时间:"${query_time}
#分割以","拼接的表名
log_types=`echo $log_type | awk -F',' '{print $0}' | sed "s/,/ /g"` 
for logtype in ${log_types}
  do
	${app_home}/${anal_dir}/load.sh ${query_time} ${logtype} ${rerun}
	${app_home}/${anal_dir}/etl.sh ${query_time} ${logtype} ${rerun}
  done

#校验etl结果是否加载到Hive分区
for logtype in ${log_types}
  do
	${HADOOP_HOME}/bin/hadoop fs -du /user/sinova/hive/warehouse/t_ods_${logtype}/dt=${day}/valid-${logtype}-${query_time}.deflate > ${log_path}/etl_checked_${logtype}.log
	file_info=`cat ${log_path}/etl_checked_${logtype}.log |grep Found`
 done

echo 'ETL执行结果加载Hive分区成功,继续执行分析程序.执行时间:'${query_time}
#分析ETL加载入hive分区的原始文件,并将分析结果导出到oracle
${app_home}/${anal_dir}/analyze.sh ${query_time} ${hql} ${tabs} ${rerun}
${app_home}/${anal_dir}/multi_export.sh ${tabs}
fi


