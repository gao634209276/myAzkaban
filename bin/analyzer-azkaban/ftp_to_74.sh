#!/bin/bash
if [ $# -lt 1 ]
then
    echo "useage: sh ftp_to_74.sh  <file_name> "
else
file_name=''
if [ $1 =  "day_ago" ] 
 then
   strdate=$(sh /home/sinova/bin/analyzer-azkaban/date_util.sh "day_ago") ##日期
   file_name=${strdate:2:6}
elif [ $1 =  "mon_ago" ]
  then
   strdate=$(sh /home/sinova/bin/analyzer-azkaban/date_util.sh "mon_ago") ##日期
   file_name=${strdate:2:6}
else
    file_name=$1
fi

ftp -n <<EOF
open 10.142.164.74
user YH_FTP ftp123$%^
bin
prom off
cd /disk/ftp/YH_FTP/jf/output
lcd  /disk1/tmp/src_logs/jf
mput *$file_name*
close
bye
EOF
 cd  /disk1/tmp/src_logs/jf
 rm *$file_name*

fi

