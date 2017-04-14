#!/bin/bash 
#######################################################################################################################
### 参数说明: $1 执行的sql文件名称 .sql $2 生成文件名称  $3 日期 yyyymmdd/yyymm $4生成文件路径                      ###
### 作用:执行sql语句，并把结果生成到文件中  							        	    ###
### 如./load_hive_file.sh jf_web.sql jf_web_001.txt   20150818  /disk1/tmp/src_logs/jf                              ###                 
#######################################################################################################################     
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
tmp_dir=$4
txt_file=""
strdate=""
if [ $3 =  "day_ago" ] 
 then
   strdate=$(sh /home/sinova/bin/analyzer-azkaban/date_util.sh "day_ago") ##日期
   if [ $4 = "/disk1/tmp/src_logs/jf" ]
     then
        txt_file=${2//day_ago/${strdate:2:6}}
     else
      txt_file=${2//day_ago/$strdate}
     fi
elif [ $3 =  "mon_ago" ]
  then
   strdate=$(sh /home/sinova/bin/analyzer-azkaban/date_util.sh "mon_ago") ##日期
   txt_file=${2//mon_ago/${strdate:0:6}}
else
   strdate=$3
   txt_file=$2
fi
echo $txt_file
echo $strdate
#执行hive sql 
$HIVE_HOME/bin/hive -f  /home/sinova/bin/analyzer-azkaban/hiveQL/$sql_file -hiveconf strdate=$strdate   > $tmp_dir/$txt_file

