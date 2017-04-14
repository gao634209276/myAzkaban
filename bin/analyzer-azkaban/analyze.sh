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
APP_HOME=$(GetConf "local.app_home")
anal_dir=$(GetConf "local.anal_dir")
HADOOP_HOME=$(GetConf "local.hadoop_home")
HIVE_HOME=$(GetConf "local.hive_home")
export HADOOP_HOME HIVE_HOME

if   [   $#   -lt 3     ]
then
    echo "useage: sh analyze.sh <query_time>  <hiveql> < hive_table_name split by ',' > <rerun>"
else
dt=''
edt=''
hour=''
len=`expr length $1`
len=`echo $1|awk '{print length($0)}'`
echo 'len:'$len
if [ ${len} -eq 8  ]
then
        dt=$1
        hour=''
fi
if [ ${len} -eq 10  ]
then
        hour=$1
        dt=${hour:0:8}
fi
if [ ${len} -eq 6  ]
then
         dt=$1
        hour=''
fi
dtstr=$1
if [[ ${dtstr}  = "hour_ago"  ]] 
then 
	hour=`date '-d 1 hour ago' +%Y%m%d%H`
	dt=${hour:0:8}
fi
if [[ ${dtstr} = "day_ago" ]] 
then
	dt=`date '-d 1 day ago' +%Y%m%d`
	hour=''
fi
if [[ ${dtstr} = "mon_ago" ]] 
then
	dt=`date -d '1 months ago' +"%Y%m"`
	hour=''
fi
edt=`date -d "$dt 1 days" +%Y%m%d`
first_day_of_month=''
first_day_of_3mon_ago=''
first_day_of_year=${dt:0:4}'0101'
curhour=`date  +%Y%m%d%H`
d3dt=`date '-d 3 day ago' +%Y%m%d`
echo ${dt}'-'${edt}'-'${hour}'-'${curhour}'-'${d3dt}
Change_Time () {
Y=`echo $1 | cut -c1-4`
M=`echo $1 | cut -c5-6`
D='01'

ChangeMonth=`echo $M-$2 |bc`
mon=''
tmp=''
if [ $ChangeMonth -lt 10 ]
then
        mon=0$ChangeMonth
else
        mon=$ChangeMonth
fi
if [ $ChangeMonth -lt 1 ]
then
        ChangeYear=`echo $Y-1 |bc`
        EndMonth=`echo 12+$ChangeMonth|bc`
       if [ $EndMonth -lt 10 ]
        then
                mon=0$EndMonth
        else
                mon=$EndMonth
        fi
         tmp=$ChangeYear$mon$D
else
         tmp=$Y$mon$D
fi
 if [ $2 == 0 ]
 then
        first_day_of_month=$tmp
 fi
if [ $2 == 2 ]
 then
        first_day_of_3mon_ago=$tmp
 fi
}
Change_Time ${dt} 0
Change_Time ${dt} 2

echo 'hour:'$hour' day:'$dt' first_day_of_month:'$first_day_of_month' first_day_of_3mon_ago:'$first_day_of_3mon_ago' first_day_of_year:'$first_day_of_year

#分割以","拼接的表名
tabs=`echo $3 | awk -F',' '{print $0}' | sed "s/,/ /g"` 
run_type=$4
#删除旧数据
if [[ $run_type = "rerun" ]]
then
	echo "手动执行日期:["${dt}"]"	
	for tab in ${tabs}
	  do
		
		ora_db=$(GetConf "remote.ora_db")
		echo "清除旧的结果表数据:库["${ora_db}"],表["${tab}"],日期["${dt}"]"
		ssh ${ora_db} "sh /export/home/oracle/exec/run.sh ${tab} ${dt} ${dt} >> /export/home/oracle/exec/run.log"
	  done
else
	echo "定时执行:"${hour}
fi

#执行Hive数据分析任务
sql_dir=${APP_HOME}/${anal_dir}/hiveQL/$2

${HIVE_HOME}/bin/hive -f ${sql_dir} -hiveconf sdt=${first_day_of_month}  -hiveconf dt=${dt} -hiveconf edt=${edt} -hiveconf m3dt=${first_day_of_3mon_ago} -hiveconf ydt=${first_day_of_year}  -hiveconf hour=${hour} -hiveconf curhour=${curhour} -hiveconf d3dt=${d3dt}
#将分析结果导入到oracle
#if [ ! -n "${tabs}" ] 
#then
#    echo "导出数据表名未指定,导出程序终止."
#else
#    echo "导出表名:["${tabs}"]"
#    for tab in ${tabs}
#    do
#	${APP_HOME}/analyzer/export.sh ${tab}
#    done
#fi
fi
