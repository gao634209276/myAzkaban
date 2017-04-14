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
export HADOOP_HOME HIVE_HOME
#######################################################################################################################
### 参数说明: $1 表名,$2 URL,$3 username ,$4 password																###
###              $5 并行数,$6并行列 $7用户名 ltyyt_st/hadoop/smsehall $8 分区名称 $9分区值 $10 where 条件     $11 导入分隔符,      ###
### 作用:从oracle 导入表到hive中 【导分区表】                                                                       ###  
#######################################################################################################################     
if   [   $#   -lt   10   ]
then
  echo   "Usage:   ora_to_hive_part.sh <table_name> [ url] [ username][ password] <parallel_count> <parallel_column> <user_name><part_key><part_value><where>"
else	
  echo  "执行从oracle导入数据.... " 
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
HIVE_TABLE_NAME=`tr '[:upper:]' '[:lower:]' <<<"$1"`
echo "HIVE_TABLE_NAME:"${HIVE_TABLE_NAME}
PARALLEL=$5
PARALLEL_COL=$6
PART_KEY=$8
#PART_VALUE=$9

#if   [   $#   -eq   10   ]
#then
#WHERE_SQL=${10}
#else 
#WHERE_SQL="1=2"
#fi

PART_VALUE=''
strdate=''
##分区替换
if [ $9 =  "day_ago" ] 
 then
   strdate=$(sh /home/sinova/bin/analyzer-azkaban/date_util.sh "day_ago") ##日期
   PART_VALUE=$strdate
   WHERE_SQL=${10//day_ago/$strdate}
elif [ $9 =  "mon_ago" ]
  then
   strdate=$(sh /home/sinova/bin/analyzer-azkaban/date_util.sh "mon_ago") ##日期
   PART_VALUE=${strdate:0:6}
   WHERE_SQL=${10//mon_ago/${strdate:0:6}}
else
   PART_VALUE=$9
   WHERE_SQL=${10}
fi

echo "======"$WHERE_SQL

HIVE_DW_HOME_DATA=${HIVE_DW_HOME}/${HIVE_TABLE_NAME}/${PART_VALUE}/
$HADOOP_HOME/bin/hadoop fs -rmr ${HIVE_DW_HOME_DATA}
ORA_TABLE_NAME=`tr '[:lower:]' '[:upper:]' <<<"$1"`
echo "HIVE_DW_HOME_DATA:"$username  
echo "&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&"$PASSWORD

fildstype=","
if   [   $#   =   11  ]
 then
  fildstype=${11}
fi

./sqoop import \
--target-dir ${HIVE_DW_HOME_DATA} \
--connect ${url} \
--username ${username}  \
--password ${password}  \
--table ${ORA_TABLE_NAME} \
--hive-table ${HIVE_TABLE_NAME} \
--where "${WHERE_SQL}" \
--hive-import \
--hive-overwrite \
--hive-partition-key ${PART_KEY} \
--hive-partition-value ${PART_VALUE} \
--fields-terminated-by ${fildstype} \
--m ${PARALLEL}  \
--split-by ${PARALLEL_COL}

fi 

