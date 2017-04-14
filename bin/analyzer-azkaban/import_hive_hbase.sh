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
HADOOP_HOME=$(GetConf "local.hadoop_home")
HIVE_HOME=$(GetConf "local.hive_home")
meta_logs=$(GetConf "local.meta_logs")
src_logs=$(GetConf "local.src_logs")
host=$(GetConf "remote.host")
uname=$(GetConf "remote.uname")
export HADOOP_HOME HIVE_HOME

sql_file=$1 ##传入sql
dt=""
strdate=""
if [ $2 =  "month_now" ] 
 then
   strdate=`date +%Y%m` ##日期
fi
echo $strdate
#执行hive sql 
$HIVE_HOME/bin/hive -f  /home/sinova/bin/analyzer-azkaban/hiveQL/$sql_file -hiveconf dt=$strdate
