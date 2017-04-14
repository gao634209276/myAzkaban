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
##########################################################################################################
### 参数说明:  $1 表名,$2 URL,$3 username ,$4 password $5列名                                          ###
###              $6 并行数,$7并行列  $8 where 条件  $9 day_ago|mon_ago(可选)                           ###
### 作用:从oracle 导入表到hive中                                                                       ### 
### 说明:导入的表在hive中可以不存在，hive自动创建表结构                                                ### 
###                    oracle表  >>>>>>> hive中表是普通表                                              ###
### 示例:./ora_to_hive.sh t_user_sdmon jdbc:oracle:thin:@10.20.11.23:1521/ecsrpt ltyyt_st ltyyt_st     ### 
###         mob,application,datetime,part 5 application part='07' mon_ago                              ###
##########################################################################################################    
if   [   $#   -lt   8   ]
then
  echo   "Usage:   ora_to_hive.sh <table_name> <url> <username> <password> <table_columns split by ','> <parallel_count> <parallel_column> <where>"
else	
  echo  "执行从oracle导入数据.... " 
  
  URL=""
  USERNAME=""
  PASSWORD=""

sqoop_home=$(GetConf "local.sqoop_home")
cd ${sqoop_home}/bin/
URL=$(GetConf "jdbc.url")
if [[ $2 != "" ]]
then
  URL=$2
  echo 'url has overwrite,not use default!'
fi
USERNAME=$(GetConf "jdbc.username")
if [[ $3 != "" ]]
then
  USERNAME=$3
  echo 'username has overwrite,not use default!'
fi
PASSWORD=$(GetConf "jdbc.password")
if [[ $4 != "" ]]
then
  PASSWORD=$4
  echo 'password has overwrite,not use default!'
fi


HIVE_DW_HOME=$(GetConf "hdfs.dw_home")
HIVE_TABLE_NAME=`tr '[:upper:]' '[:lower:]' <<<"$1"`
echo "HIVE_TABLE_NAME:"${HIVE_TABLE_NAME}

  HIVE_DW_HOME_DATA=${HIVE_DW_HOME}/${HIVE_TABLE_NAME}/

$HADOOP_HOME/bin/hadoop fs -rmr ${HIVE_DW_HOME_DATA}

ORA_TABLE_NAME=`tr '[:lower:]' '[:upper:]' <<<"$1"`

COLUMNS=$5

PARALLEL=$6
PARALLEL_COL=$7


##分区替换
  if   [   $#   -eq   9   ]
   then
     if [ $9 =  "day_ago" ] 
       then
          strdate=$(sh /home/sinova/bin/analyzer-azkaban/date_util.sh "day_ago") ##日期
          WHERE_SQL=${8//day_ago/$strdate}
      elif [ $9 =  "mon_ago" ]
        then
        echo "***********"$9
          strdate=$(sh /home/sinova/bin/analyzer-azkaban/date_util.sh "mon_ago") ##日期
          WHERE_SQL=${8//mon_ago/$strdate}
      fi
 
  else
    WHERE_SQL=${8}
  fi

./sqoop import \
--target-dir ${HIVE_DW_HOME_DATA} \
--connect ${URL}  \
--username ${USERNAME}  \
--password ${PASSWORD} \
--append \
--table ${ORA_TABLE_NAME} \
--columns ${COLUMNS} \
--create-hive-table \
--hive-table ${HIVE_TABLE_NAME} \
--where "${WHERE_SQL}" \
--input-fields-terminated-by  ',' \
--null-string '' --null-non-string '' \
--m ${PARALLEL} \
--split-by ${PARALLEL_COL}

echo ":::::::::::CREATE HIVE EXTERNAL TABLE MAPPING ON  IMPORTED DATA PATH:::::::::::"
${HIVE_HOME}/bin/hive -e "drop table if exists  ${HIVE_TABLE_NAME}"

OLD_IFS="$IFS"
IFS=","
arr=($COLUMNS)
IFS="$OLD_IFS"
COLS=""
for s in ${arr[@]}
do
	COLS=$COLS"$s  string,"
done
echo ${COLS}
COLS=${COLS%,*}
echo ${COLS}
create_sql="CREATE EXTERNAL TABLE ${HIVE_TABLE_NAME}(${COLS})row format delimited fields terminated by ',' LOCATION '${HIVE_DW_HOME_DATA}'"

${HIVE_HOME}/bin/hive -e "${create_sql}"
fi
