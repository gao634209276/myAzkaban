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

if   [[ $# -lt 1 ]]
then
  echo   "Usage:   export.sh <table_name> [ url] [ username][ password]"
else	
  echo   "执行导出分析结果到DB! "  
sqoop_home=$(GetConf "local.sqoop_home")
cd ${sqoop_home}/bin/
url=$(GetConf "jdbc.url")
if [[ $2 != "" ]]
then
  url=$2
  echo 'url has overwrite,not use default!'
fi
username=$(GetConf "jdbc.username")
if [[ $3 != "" ]]
then
  username=$3
  echo 'username has overwrite,not use default!'
fi
password=$(GetConf "jdbc.password")
if [[ $4 != "" ]]
then
  password=$4
  echo 'password has overwrite,not use default!'
fi
HIVE_DW_HOME=$(GetConf "hdfs.dw_home")
echo "sqoop_home:"${sqoop_home}
str=$1
HIVE_TABLE_NAME=`tr '[:upper:]' '[:lower:]' <<<"$1"`
echo "HIVE_TABLE_NAME:export-"${HIVE_TABLE_NAME}

ORA_TABLE_NAME=`tr '[:lower:]' '[:upper:]' <<<"$str"`

./sqoop export \
--connect ${url}  \
--table ${ORA_TABLE_NAME} \
--username ${username}  \
--password ${password} \
--export-dir ${HIVE_DW_HOME}/${HIVE_TABLE_NAME} \
--input-fields-terminated-by  '\001' \
--input-null-string "\\\\N" \
--input-null-non-string "\\\\N" 
fi
