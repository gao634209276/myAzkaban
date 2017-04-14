#!/bin/bash

help_info(){ 
    echo "./ftp.sh -t get|put -h ip_host -u username -p password  -f from_path -d destination_path -a time_arg"
}
if [ $# -eq 0 ] 
then
    help_info
else
type=""
host=""
username=""
pwd=""
from_path=""
dest_path=""
time_arg=""
    while getopts a:t:h:u:p:f:d: ARGS  
    do  
    case $ARGS in   
	a)  
	    time_arg=$OPTARG
            ;; 
        t)  
	    type=$OPTARG
            ;;  
        h)  
	    host=$OPTARG
            ;;  
        u)  
	   username=$OPTARG
            ;;  
         p)  
	   pwd=$OPTARG
            ;; 
         f)  
	   from_path=$OPTARG
            ;;
         d)  
	   dest_path=$OPTARG
            ;;	    
	  *)  
             echo "Unknow option: $ARGS"  
            ;;
    esac  
    done  

if [[ $time_arg = "hour_ago" ]]
then
	time_arg=`date '-d 1 hour ago' +%Y%m%d%H`
elif [[ $time_arg = "day_ago" ]]
then
	time_arg=`date '-d 1 day ago' +%Y%m%d`
elif [[ $time_arg = "month_ago" ]]
then
	time_arg=`date '-d 1 month ago' +%Y%m`
fi

 echo $time_arg" "$type" "$host" "$username" "$pwd " "$from_path" "$dest_path

if [[ ${type} = "get" ]];
then
	mkdir -p ${dest_path}
	ftp -n -i -v ${host} <<-END
	user ${username} ${pwd}
	binary
	cd ${from_path}
	ls "-lrt *${time_arg}*"
	lcd ${dest_path}
	mget *${time_arg}*
	bye
	END
#统计文件信息
cd ${dest_path}
f_count=`ls ${dest_path}/* |wc -l`
rec_count=`cat ${dest_path}/* |wc -l`
f_size=`du -hs`
echo "###Get data from ftpServer['${host}${from_path}']:fileCount[${f_count}],fileSize[${f_size}],recoredCount[${rec_count}]"
#打印azkaban监控扫描信息
echo 'HIVE_TABLE_NAME:FTP-['${host}']-GET'
echo 'Exported '${rec_count}' records.'

elif [[ $type = "put" ]];
then
	ftp -n -i -v ${host} <<-EOF
	user ${username} ${pwd}
	binary
	lcd ${from_path}
	cd ${dest_path}
	mput *
	bye
	EOF

#统计文件信息
cd ${from_path}
f_count=`ls ${from_path}/* |wc -l`
rec_count=`cat ${from_path}/* |wc -l`
f_size=`du -hs`
echo "###Put data to ftpServer['${host}${dest_path}']:fileCount[${f_count}],fileSize[${f_size}],recoredCount[${rec_count}]"
rm ${from_path}/*
#打印azkaban监控扫描信息
echo 'HIVE_TABLE_NAME:FTP-['${host}']-PUT'
echo 'Exported '${rec_count}' records.'
else
	echo "Unknow Action!,Use get or put args!"
fi
fi
