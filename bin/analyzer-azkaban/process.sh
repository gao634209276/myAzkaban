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
if   [   $#   -lt   3   ]
then
        echo   "Usage:   process.sh {query_time,local_dir,file_suffix,file_prefix}"
else
query_time=$1
local_dir=$2
file_suffix=$3
file_prefix=$4

len=`echo $1|awk '{print length($0)}'`
#默认提取日期
if [[ $1  = "hour_ago"  ]] 
then 
	dts=`date '-d 1 hour ago' +%Y%m%d%H`
	day=${dts:0:8}
elif [ ${len} -eq 10  ]
then
	dts=$1
	day=${dts:0:8}
elif [[ $1 = "day_ago" ]] 
then
	dts=`date '-d 1 day ago' +%Y%m%d`
	day=${dts}
elif [ ${len} -eq 8  ]
then
        dts=$1
        day=$1

fi
echo "dts:"${dts}"     day:"${day}
file_name=${file_prefix}${dts}${file_suffix}
#cat $local_dir/* > ${dts}${file_name}
cd ${local_dir}
cat * > ${file_name}
find ${local_dir} | grep -v ${file_name} | xargs rm
totalNum=`cat $local_dir/${file_name}|wc -l`
file_size=`du $local_dir/${file_name} -lh |cut -f1`
echo "生成文件总记录条数:"$totalNum
echo "生成文件总大小:"$file_size

#azkaban任务调度监控点
echo 'HIVE_TABLE_NAME:process-'${file_name}
echo 'Exported '$totalNum' records.'

numTemp="0000000000"
b=${#totalNum}
c=$(( 10-$b))
d=`echo $numTemp|cut -c1-${c}`
totalNum="$d$totalNum"
echo $totalNum

#sed -i "1i${totalNum}"  $local_dir/${file_name}
#sed -i '1G' $local_dir/${file_name}
fi
