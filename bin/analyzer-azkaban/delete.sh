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
if   [   $#   -lt   2   ]
then
        echo   "Usage: delete.sh {run_type,query_time,tab}"
else
run_type=$1
query_time=$2
tab=$3

#默认提取日期
if [[ $2 = "day_ago" ]] 
then
	dts=`date '-d 1 day ago' +%Y%m%d`
	day=${dts}
else
        dts=$1
        day=$1
fi
echo "dts:"${dts}"     day:"${day}
ora_db=$(GetConf "remote.ora_db")
if [[ $run_type = "pay" ]]
then
	ssh ${ora_db} "sh /export/home/oracle/exec/product_run.sh DEL ${run_type} ${day} ${day} >> /export/home/oracle/exec/run.log"
elif [[ $run_type = "shop" ]]
then
	ssh ${ora_db} "sh /export/home/oracle/exec/product_run.sh DEL ${run_type} ${day} ${day} >> /export/home/oracle/exec/run.log"
elif [[ $run_type = "normal" ]]
then
	ssh ${ora_db} "sh /export/home/oracle/exec/run.sh ${tab} ${day} ${day} >> /export/home/oracle/exec/run.log"
fi
fi
