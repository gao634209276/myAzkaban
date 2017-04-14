#!/bin/bash
######################################################################################################################
### 参数说明: $1 encryt 加密，descrypt 解密  $2 原始文件目录  $3:新文件目录  $4 day_ago|mon_ago|可选               ###
### 作用:加密，解密文件 	                                                                                   ###            
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
java_home=$(GetConf "local.java_home")
export LANG="en_US.UTF-8"

if   [   $#   -lt   4   ]
then
        echo   "Usage: file_aes.sh {encryt|descrypt,sourcePath,newPath,day_ago}"
else
   sorce_path=$2
   new_path=$3
if [ $4 =  "day_ago" ] 
 then
   strdate=$(sh /home/sinova/bin/analyzer-azkaban/date_util.sh "day_ago") ##日期
    sorce_path=${2//day_ago/$strdate}
        new_path=${3//day_ago/$strdate}
elif [ $4 =  "day_ago4" ]
  then
   strdate=$(sh /home/sinova/bin/analyzer-azkaban/date_util.sh "day_ago") ##日期
    sorce_path=${2//day_ago/${strdate:2:6}} 
       new_path=${3//day_ago/${strdate:2:6}}

fi

 cd /home/sinova/bin/analyzer-azkaban/java/FileAes/bin/util
${java_home}/bin/java -cp /home/sinova/bin/analyzer-azkaban/java/FileAes/lib/commons-codec-1.4.jar:/home/sinova/bin/analyzer-azkaban/java/FileAes/bin/ util.AESTester $1 $sorce_path $new_path

rm $sorce_path

fi
