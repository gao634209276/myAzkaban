#!/bin/bash
if [ $# -lt 2 ]
then
    echo "useage: sh ftp_baobiao7.sh <file_path> <file_name> "
else
file_path=$1
file_name=''
if [ $2 =  "day_ago" ] 
 then
   strdate=$(sh /home/sinova/bin/analyzer-azkaban/date_util.sh "day_ago") ##日期
   file_name=${strdate:2:6}
elif [ $2 =  "mon_ago" ]
  then
   strdate=$(sh /home/sinova/bin/analyzer-azkaban/date_util.sh "mon_ago") ##日期
   file_name=${strdate:2:6}
else
    file_name=$2
fi

ftp -n <<EOF
open 10.142.164.7
user sinova L-x43*~rZm
bin
prom off
cd /app/sinova/webehall/$file_path
lcd  /disk1/tmp/src_logs/$file_path
mput *$file_name*
close
bye
EOF
fi

