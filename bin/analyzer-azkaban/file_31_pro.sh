#!/bin/bash
######################################################################################################################
### 参数说明: $1 执行的sql文件名称 .sql $2 生成文件名称  $3 日期 yyyymmdd|yyymm|day_ago|mon_ago                    ###
### 作用:执行sql语句，并把结果分别生成到31个省份文件中  	                                                   ###
###  本地目录：/disk2/ftpfile/省份编码	 						   		           ###
### 如./file_31_pro.sh wap_3glogin.sql wap_3glogin_yyyy-mm.txt   mon_ago                                           ###               
######################################################################################################################  
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


sql_file=$1 #执行sql文件名 xxx.sql
out_file="" #生成文件名称 xxx_yyyymmdd.txt ,xxx_yyyy-mm-dd.txt
dt=""

#默认提取日期
if [[ $3 = "day_ago" ]] 
then
	dt=$(sh /home/sinova/bin/analyzer-azkaban/date_util.sh "day_ago") 
	dt_y=${2//yyyy/${dt:0:4}}
	dt_m=${dt_y/mm/${dt:4:2}}
	dt_d=${dt_m/mm/${dt:6:2}}
	out_file=$dt_d
	
elif [ $3 =  "mon_ago" ] || [ $3 = "month_ago" ]
  then
    dt=$(sh /home/sinova/bin/analyzer-azkaban/date_util.sh "mon_ago") ##日期
   	dt=${dt:0:6}
        dt_y=${2//yyyy/${dt:0:4}}
	dt_m=${dt_y/mm/${dt:4:2}}
	out_file=$dt_m
else
        dt=$3
	out_file=$2
fi


sqldir=/home/sinova/bin/analyzer-azkaban/hiveQL ##本地sql语句目录
local_dir=/disk2/ftpfile  ##本地文件生成目录


FILE_LIST="010  013  018  030  034  038  051  070  074  076  081  084  086  088  090  097 011  017  019  031  036  050  059  #071  075  079  083  085  087  089  091"
#FILE_LIST="010"
#执行hive sql 
for FILE_ID in ${FILE_LIST}
  do
echo $FILE_ID;
#echo ${out_file}
#echo ${dt}
##开始生成文件
$HIVE_HOME/bin/hive -f $sqldir/$sql_file  -hiveconf pid=${FILE_ID}  -hiveconf dt=${dt} > $local_dir/${FILE_ID}/${FILE_ID}_${out_file}

##文件同步 下发省份FTP
FILE_PATH_P="/app/data"
R_IP_P="10.20.33.12"
USRNAME_P="sinovatech"
PASSWD_P="1pov^St7B"

ftp -n<<EOF
open $R_IP_P
user $USRNAME_P $PASSWD_P
bin
prom off
cd  /${FILE_ID}/
lcd ${local_dir}/${FILE_ID}/
mput *_${out_file}*
close

bye
EOF


#清理本地临时目录
#rm -f  $local_dir/${FILE_ID}/*${y_m}.txt

 
done
