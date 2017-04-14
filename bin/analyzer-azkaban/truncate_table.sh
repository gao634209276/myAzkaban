########################################################################################
#### 把报表 删除表的分区                                                             ###
#### 参数：参数1 表名称                                                              ###
####       参数2 分区列名                                                            ###
####       参数3 分区名称 day_ago|mon_ago                                            ###
####       参数4 按月循环删除 mon|day                                                ###
########################################################################################
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
export LANG="en_US.UTF-8"
if   [   $#   !=   4   ]
then
        echo   "Usage: truncate_table {table_name,分区字段,分区名称(day_ago|mon_ago|其它),分区类型(mon|day"
else
    echo $1 $2 $3 $4
    table_name=$1
    part_name=$2
    part_value=$3
    datestr=""
  if [ $3 =  "day_ago" ] 
  then
   date_str=$(sh /home/sinova/bin/analyzer-azkaban/date_util.sh "day_ago") ## yyyymmdd
  elif [ $3 =  "mon_ago" ]
  then
   date_str=$(sh /home/sinova/bin/analyzer-azkaban/date_util.sh "mon_ago") ## yyyymm
  else
   date_str=$3
  fi   

    if [ $4 = "day" ] 
       then
          echo " ALTER TABLE $table_name DROP IF EXISTS PARTITION($part_name=$date_str) "
         $HIVE_HOME/bin/hive -S -e " ALTER TABLE $table_name DROP IF EXISTS PARTITION($part_name=$part_value) "
     elif [ $4 = "mon" ]
        then
        first_day=${date_str:0:6}'01'
        first_last=`date -d "$first_day 1 months" +%Y%m`'01' ##下个月1号 
        end_day=`date -d "$first_last -1 days" +%Y%m%d`
         echo  $first_day $end_day
       
        datestring=""
	st=`date -d "$first_day" +%s`
	et=`date -d "$end_day" +%s`
	while [ "$st" -le "$et" ]
  	 do
     	   	datestring=`date -d @$st +%F`
     		dstr=`date -d "$datestring" +"%Y%m%d"`
     	       echo " ALTER TABLE $table_name DROP IF EXISTS PARTITION($part_name=$dstr) "
        	  $HIVE_HOME/bin/hive -S -e " ALTER TABLE $table_name DROP IF EXISTS PARTITION($part_name=$dstr) "
                st=$((st+86400))
 	 done
  
    fi

fi

