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
app_home=$(GetConf "local.app_home")
#分割以","拼接的表名
tabs=`echo $1 | awk -F',' '{print $0}' | sed "s/,/ /g"` 
#将分析结果导入到oracle
if [ ! -n "${tabs}" ] 
then
    echo "导出数据表名未指定,导出程序终止."
else
    echo "导出表名:["${tabs}"]"
    for tab in ${tabs}
    do
	  ${app_home}/analyzer-azkaban/export.sh ${tab}
    done
fi
