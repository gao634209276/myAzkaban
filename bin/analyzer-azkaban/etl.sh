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
anal_dir=$(GetConf "local.anal_dir")
src_logs=$(GetConf "local.src_logs")
etl_errs=$(GetConf "local.etl_errs")
mkdir -p ${etl_errs}
HADOOP_HOME=$(GetConf "local.hadoop_home")
HIVE_HOME=$(GetConf "local.hive_home")
export HADOOP_HOME HIVE_HOME

if   [   $#   -lt   2   ]
then
        echo   "Usage:   etl.sh <date(20120101)> <log_type>"
else
if [ $1 = "hour_ago" ]
then
	date=`date '-d 1 hour ago' +%Y%m%d%H`
elif [ $1 = "day_ago" ]
then
	date=`date '-d 1 day ago' +%Y%m%d`
else
	date=$1
fi
#date=$1
log_type=$2
len=`expr length $date` 
len=`echo $date|awk '{print length($0)}'` 

if [ ${len} -gt 8 ]; then
	day=${date:0:8}
else
	day=${date}
fi
#etl路径配置
main_jar=$(GetConf "mr.main_jar")
sys_conf=${app_home}/${anal_dir}/conf/sys_conf.ini
etl_conf=${app_home}/${anal_dir}/conf/etl/${log_type}_etl.xml
merged_file=${date}.merged
local_path=${src_logs}/${log_type}/${day}
mkdir -p ${local_path}
hdfs_input=$(GetConf "hdfs.in")/${log_type}/${day}
hdfs_output=$(GetConf "hdfs.out")/${log_type}/${day}
hive_warehouse=$(GetConf "hdfs.dw_home")
dt="dt=${day}"
#合并所有目录下的文件
cat ${local_path}/*${date}*.log >>  ${local_path}/${merged_file}
rm -rf ${local_path}/*.log
#上传合并后的文件到hdfs对应目录
${HADOOP_HOME}/bin/hadoop fs -mkdir -p ${hdfs_input}
${HADOOP_HOME}/bin/hadoop fs -rm ${hdfs_input}/*

${HADOOP_HOME}/bin/hadoop fs -put ${local_path}/${merged_file} ${hdfs_input}/
rm -rf ${local_path}/${merged_file}
#清理分区旧数据
run_type=$3
if [[ ${run_type} = "rerun" ]]
then
	h_tab_name="t_ods_"${log_type}
	echo "手动执行日期:["$1"],hive_table_name:["${h_tab_name}"]"
	${HADOOP_HOME}/bin/hadoop fs -rm ${hive_warehouse}/${h_tab_name}/${dt}/*${date}*
	${HADOOP_HOME}/bin/hadoop fs -rm ${hive_warehouse}/t_etl_errs_log/${dt}/*${log_type}-${date}*
else
	echo "定时执行"
fi

#执行ETL主程序
if [[ ${log_type} = "detail" ]]
then
${HADOOP_HOME}/bin/hadoop jar ${app_home}/jars/LogEtl_detail.jar  com.sinova.dw.etl.service.EtlZhfx ${hdfs_input} ${etl_conf} ${sys_conf} ${date}
else
${HADOOP_HOME}/bin/hadoop jar ${main_jar}  com.sinova.dw.etl.service.EtlMRv2 0 ${sys_conf} ${etl_conf} ${hdfs_input} ${hdfs_output} ${log_type}-${date} ${run_type}
fi
h_tab_name="t_ods_"${log_type}
etl_count=`${HADOOP_HOME}/bin/hadoop fs -cat ${hive_warehouse}/${h_tab_name}/${dt}/*${date}* | wc -l`
#azkaban任务调度监控点
echo 'HIVE_TABLE_NAME:etl-'$2'_'$1
echo 'Exported '$etl_count' records.'
#删除临时输入文件
${HADOOP_HOME}/bin/hadoop fs -rm -r ${hdfs_input}
#加载错误日志进错误表分区
err_count=0
err_count=`${HADOOP_HOME}/bin/hadoop fs -cat ${hdfs_output}/err* | wc -l`
if [ $err_count -gt 0 ]
then
	${HIVE_HOME}/bin/hive -e "LOAD DATA INPATH '${hdfs_output}/err*'  INTO TABLE t_etl_errs_log PARTITION (dt='${day}')"
fi
fi
