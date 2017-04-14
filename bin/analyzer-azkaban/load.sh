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
app_home=$(GetConf "local.app_home")
src_logs=$(GetConf "local.src_logs")
mkdir -p ${src_logs}
meta_logs=$(GetConf "local.meta_logs")
mkdir -p ${meta_logs}
host=$(GetConf "remote.host")
uname=$(GetConf "remote.uname")
remote_root=$(GetConf "remote.root")
if   [   $#   -lt 2     ]
then
    echo "usage: sh load_data.sh <date> <log_type> <run_type>"
else
hour=""
hosts_ip=""
sub_dir=""
log_type=$2
if [ $1 = "hour_ago" ]
then
	hour=`date '-d 1 hour ago' +%Y%m%d%H`
elif [ $1 = "day_ago" ]
then
	hour=`date '-d 1 day ago' +%Y%m%d`
else
	hour=$1
fi

echo "hour:"${hour}
day=${hour:0:8}
local_dir="${src_logs}/${log_type}/${day}"
mkdir -p  ${local_dir}
log_file="*${hour}*.log"

stime=$(date +%s -d 'now')
#根据日志类型,指定查找的目录
if [ ${log_type} = "23gsms051" ]
then
        rm ${src_logs}/${log_type}/${day}/*
        root_dir="/disk/ftp/YH_FTP/23gsms051"
	log_file="*${hour}*"
        echo  ${uname}@${host}:${root_dir}/${log_file}
        scp ${uname}@${host}:${root_dir}/${log_file} ${local_dir}
        cd ${local_dir}
	for files in `ls *.*`
	do
		if [ "${files##*.}" = "gz"  ]
                then
			echo 'gunzip' $files
                        gunzip $files
                fi
        done
	echo 'merge files...'
	iconv -f gbk -t utf-8 -c ${local_dir}/*.txt > ${local_dir}/${hour}.log
	echo 'rm '${local_dir}'/*.txt'
	rm ${local_dir}/*.txt
fi
if [ ${log_type} = "pv_wap" ]
then
	rm -r ${src_logs}/${log_type}
	mkdir -p  ${local_dir}
	touch ${local_dir}/empty.log
        root_dir="/disk3/YH_DATA/DATA/mobile2.0/pv"
        echo  ${uname}@${host}:${root_dir}/${log_file}
        scp ${uname}@${host}:${root_dir}/${log_file} ${local_dir}
	cd ${local_dir}
fi
if [ ${log_type} = "detail" ]
then
	root_dir="/disk3/YH_DATA/DATA/web2.0/detail/sediment"
        echo  ${uname}@${host}:${root_dir}/${log_file}
        scp ${uname}@${host}:${root_dir}/${log_file} ${local_dir}
fi
if [ ${log_type} = "biz_web" ]
then
	root_dir="/disk3/YH_DATA/DATA/web2.0"
	sub_dir="info"
	hosts_ip=""
	echo  ${uname}@${host}:${root_dir}/${sub_dir}/${log_file}
   	scp ${uname}@${host}:${root_dir}/${sub_dir}/${log_file} ${local_dir}
fi
if [ ${log_type} = "biz_intf"  ]
then
	#网厅2.0业务接口日志目录
	root_dir="/disk3/YH_DATA/DATA/web2.0"
	sub_dir="interface"
	echo  ${uname}@${host}:${root_dir}/${sub_dir}/${log_file}
	scp ${uname}@${host}:${root_dir}/${sub_dir}/${log_file} ${local_dir}
	
	#手厅2.0业务接口日志目录
	root_dir="/disk3/YH_DATA/DATA/mobile2.0"
	sub_dir="/interface/*"
	hosts_ip="10.20.34.11 10.20.34.12 10.20.34.13 10.20.34.14 10.142.164.181 10.142.164.182 10.20.8.17 10.20.8.18 10.20.8.19 10.20.8.20 10.142.194.25 10.142.194.26 10.142.194.27 10.142.194.28 10.142.194.29 10.142.194.30 10.142.194.31 10.142.194.32 10.142.194.33 10.142.194.34 "	

	for IP in ${hosts_ip}
	  do
		echo  ${uname}@${host}:${root_dir}/${IP}${sub_dir}/${log_file}
		scp ${uname}@${host}:${root_dir}/${IP}${sub_dir}/${log_file} ${local_dir}
	done
hosts_ip=""
fi
if [ ${log_type} = "ad" ]
then
	root_dir="/disk3/YH_DATA/DATA"
	sub_dir="/app/tomcat/ad/uniAdms"
	hosts_ip="10.20.14.11 10.20.14.12 10.20.14.13 10.20.14.14"	
fi
if [ ${log_type} = "sign_in" ]
then
        root_dir="/disk/ftp/YH_FTP/sign"
        sub_dir=""
        hosts_ip=""
echo   ${uname}@${host}:${root_dir}/${sub_dir}/signin${log_file}  ${local_dir}
 scp ${uname}@${host}:${root_dir}/${IP}${sub_dir}/signin${log_file}  ${local_dir}
fi

if [ ${log_type} = "sign_prize" ]
then
        root_dir="/disk/ftp/YH_FTP/sign"
        sub_dir=""
        hosts_ip=""
echo   ${uname}@${host}:${root_dir}/${sub_dir}/prize${log_file}  ${local_dir}
 scp ${uname}@${host}:${root_dir}/${IP}${sub_dir}/prize${log_file}  ${local_dir}
fi

if [ ${log_type} = "biz_mob" ]
then
	root_dir="/disk3/YH_DATA/DATA/mobile2.0"
	sub_dir="/info/*"
	hosts_ip="10.20.34.11 10.20.34.12 10.20.34.13 10.20.34.14 10.20.34.15 10.20.34.18 10.142.164.181 10.142.164.182 10.20.8.17 10.20.8.18 10.20.8.19 10.20.8.20 10.142.194.25 10.142.194.26 10.142.194.27 10.142.194.28 10.142.194.29 10.142.194.30 10.142.194.31 10.142.194.32 10.142.194.33 10.142.194.34 132.35.102.219 132.35.102.220 132.35.102.239 132.35.102.241 132.35.102.242 132.35.102.243"	
fi
if [ ${log_type} = "client" ]
then
	root_dir="/disk3/YH_DATA/DATA/mobile2.0"
	sub_dir="/client/*"
	hosts_ip="10.20.34.11 10.20.34.12 10.20.34.13 10.20.34.14 10.142.164.181 10.142.164.182 10.20.8.17 10.20.8.18 10.20.8.19 10.20.8.20 10.142.194.25 10.142.194.26 10.142.194.27 10.142.194.28 10.142.194.29 10.142.194.30 10.142.194.31 10.142.194.32 10.142.194.33 10.142.194.34"	
fi
if [ ${log_type} = "fav" ]
then
	root_dir="/disk3/YH_DATA/DATA/mobile2.0"
	sub_dir="/favorite/*"
	hosts_ip="10.20.34.11 10.20.34.12 10.20.34.13 10.20.34.14 10.142.164.181 10.142.164.182 10.20.8.17 10.20.8.18 10.20.8.19 10.20.8.20 10.142.194.25 10.142.194.26 10.142.194.27 10.142.194.28 10.142.194.29 10.142.194.30 10.142.194.31 10.142.194.32 10.142.194.33 10.142.194.34"	
fi
if [ ${log_type} = "push" ]
then
	root_dir="/disk3/YH_DATA/DATA/mobile2.0"
	sub_dir="/*/*"
	hosts_ip="10.10.34.17 10.10.34.18 10.20.34.19 10.20.34.20 10.20.13.13 10.20.13.14"	
fi
 if [ ${log_type} = "pv" ]
then
	rm -r ${src_logs}/${log_type}
	mkdir -p  ${local_dir}
	root_dir="/disk3/YH_DATA/DATA/mobile2.0"
	sub_dir="/pv/*"
	hosts_ip="10.20.34.11 10.20.34.12 10.20.34.13 10.20.34.14 10.142.194.25 10.142.194.26 10.142.194.27 10.142.194.28 10.142.194.29 10.142.194.30 10.142.194.31 10.142.194.32 10.142.194.33 10.142.194.34"	
fi

if [ ${log_type} = "biz_sms_sfts" ]
then
	root_dir="/disk/ftp/YH_FTP"
	sub_dir="/app/tomcat/logs/simulate/dataplatformlog/data"
	hosts_ip="10.142.132.50 10.142.132.51 10.142.132.52 10.20.39.35 10.20.39.36 10.20.39.37 10.20.39.38 10.20.39.39 10.20.39.40 10.20.39.41 10.20.39.42 10.20.39.43 10.20.39.44 10.20.39.45 10.20.39.46"	
fi
if [ ${log_type} = "biz_sms" ]
then
	root_dir="/disk/ftp/YH_FTP"
	sub_dir="/app/tomcat/logs/sms/dataplatformlog/data"
	sub_dir1="/app/tomcat/logs/sms2/dataplatformlog/data"
        hosts_ip="10.142.132.41 10.142.132.42 10.142.132.43 10.142.132.44 10.142.132.45 10.142.132.46 10.142.132.47 10.142.132.48 10.20.39.11 10.20.39.12 10.20.39.13 10.20.39.14 10.20.39.15 10.20.39.16 10.20.39.17 10.20.39.18 10.20.39.19 10.20.39.20 10.20.39.21 10.20.39.22 10.20.39.23 10.20.39.24 10.20.39.25 10.20.39.26 10.20.39.27 10.20.39.28 10.20.39.29 10.20.39.30"	
	for IP in ${hosts_ip}
	  do
		echo  ${uname}@${host}:${root_dir}/${IP}${sub_dir}/${log_file}
		scp ${uname}@${host}:${root_dir}/${IP}${sub_dir}/${log_file} ${local_dir}
	done
	cd ${local_dir}
	 for files in `ls *.log`
	  do 
		mv $files sms0_${files}
	done
	for IP in ${hosts_ip}
	  do
		echo  ${uname}@${host}:${root_dir}/${IP}${sub_dir1}/${log_file}
		scp ${uname}@${host}:${root_dir}/${IP}${sub_dir1}/${log_file} ${local_dir}
	done

hosts_ip=""
fi

if [ ${log_type} = "nginx" ]
then
	root_dir="/disk3/YH_DATA/DATA"
	sub_dir="/app/nginx/logs"
	sub_dir1="/app/nginx2/logs"
        hosts_ip="10.10.15.11 10.10.15.12 10.10.15.13 10.10.15.14 10.10.15.15 10.10.15.16 10.10.15.17 10.10.15.18"	
	for IP in ${hosts_ip}
	  do
		echo  ${uname}@${host}:${root_dir}/${IP}${sub_dir}/${log_file}
		scp ${uname}@${host}:${root_dir}/${IP}${sub_dir}/${log_file} ${local_dir}
	done
        cd ${local_dir}
         for files in `ls *.log`
          do
		echo $files 'nginx_'${files}
                mv $files nginx_${files}
        done
        for IP in ${hosts_ip}
          do
                echo  ${uname}@${host}:${root_dir}/${IP}${sub_dir1}/${log_file}
                scp ${uname}@${host}:${root_dir}/${IP}${sub_dir1}/${log_file} ${local_dir}
        done
	cd ${local_dir}
	rm nginx-*.log
	 for files in `ls *.log`
	  do 
		cat $files | grep -v "f5status" | grep -v "favicon.ico" | grep -v "/js/" | grep -v "/images/" | grep -v ".css" | grep -v ".js" | grep -v ".gif" | grep -v ".png" | grep -v "/weather/" | grep -v "/sinova.jsp"  | grep -v ".jpg" | grep -v ".swf" | grep -v "/reminder2/" | grep -v "/navhtml3/" | grep -v "sitestat" >> nginx-${hour}.log
		rm $files
	done
	hosts_ip=""
fi
if [ ${log_type} = "login" ]
then
	root_dir="/disk/ftp/YL_FTP"
	sub_dir="/tyrz/"
	hosts_ip=""

	echo  ${uname}@${host}:${root_dir}/${sub_dir}/${log_file}
   	scp ${uname}@${host}:${root_dir}/${sub_dir}/${log_file} ${local_dir}
fi
if [ ${log_type} = "biz_auto" ]
then
	root_dir="/disk/ftp/YL_FTP"
	sub_dir="/zzfw/"
	hosts_ip=""

	echo  ${uname}@${host}:${root_dir}/${sub_dir}/${log_file}
   	scp ${uname}@${host}:${root_dir}/${sub_dir}/${log_file} ${local_dir}
fi
if [ ${log_type} = "shortaddress" ]
then
	root_dir="/disk/ftp/YH_FTP/shortaddress/input"
	sub_dir="/*"
	hosts_ip="10.10.13.19 10.10.13.20"

echo   ${uname}@${host}:${root_dir}/${IP}${sub_dir}/${log_file} ${local_dir}

fi
if [ ${log_type} = "search" ]
then
	root_dir="/disk/ftp/YH_FTP"
	sub_dir="search"
	hosts_ip=""

echo   ${uname}@${host}:${root_dir}/${IP}${sub_dir}/${log_file} ${local_dir}
  	scp ${uname}@${host}:${root_dir}/${IP}${sub_dir}/${log_file} ${local_dir}
fi
#if [ ${log_type} = "biz_intf_v3" ]
#then
#	root_dir="/disk3/YH_DATA/DATA/web3.0"
#	sub_dir="hadooplog"
#	hosts_ip=""

#echo   ${uname}@${host}:${root_dir}/${sub_dir}/interface${log_file}  ${local_dir}
# scp ${uname}@${host}:${root_dir}/${IP}${sub_dir}/interface${log_file}  ${local_dir}
#fi
if [ ${log_type} = "biz_web_v3" ]
then
	root_dir="/disk3/YH_DATA/DATA/web3.0"
	sub_dir="hadooplog"
	hosts_ip=""

echo   ${uname}@${host}:${root_dir}/${sub_dir}/service${log_file}  ${local_dir}
 scp ${uname}@${host}:${root_dir}/${IP}${sub_dir}/service${log_file}  ${local_dir}
fi
if [ ${log_type} = "biz_intf_v3" ]
then
	root_dir="/disk3/YH_DATA/DATA/service3.0"
    sub_dir="interface"
	hosts_ip=""

echo   ${uname}@${host}:${root_dir}/${sub_dir}/interface${log_file}  ${local_dir}
 scp ${uname}@${host}:${root_dir}/${IP}${sub_dir}/interface${log_file}  ${local_dir}

       root_dir="/disk3/YH_DATA/DATA/web3.0"
       sub_dir="hadooplog"
       hosts_ip=""

echo   ${uname}@${host}:${root_dir}/${sub_dir}/interface${log_file}  ${local_dir}
 scp ${uname}@${host}:${root_dir}/${IP}${sub_dir}/interface${log_file}  ${local_dir}

fi
if [ ${log_type} = "biz_v3" ]
then
     	root_dir="/disk3/YH_DATA/DATA/service3.0"
        sub_dir="service"
        hosts_ip=""

echo   ${uname}@${host}:${root_dir}/${sub_dir}/service${log_file}  ${local_dir}
 scp ${uname}@${host}:${root_dir}/${IP}${sub_dir}/service${log_file}  ${local_dir}
fi
if [ ${log_type} = "biz_v3_new" ]
then
        root_dir="/disk3/YH_DATA/DATA/service_new"
        sub_dir="service"
        hosts_ip=""

echo   ${uname}@${host}:${root_dir}/${sub_dir}/service${log_file}  ${local_dir}
 scp ${uname}@${host}:${root_dir}/${IP}${sub_dir}/service${log_file}  ${local_dir}
fi
if [ ${log_type} = "biz_intf_v3_new" ]
then
        root_dir="/disk3/YH_DATA/DATA/service_new"
        sub_dir="interface"
        hosts_ip=""

echo   ${uname}@${host}:${root_dir}/${sub_dir}/interface${log_file}  ${local_dir}
 scp ${uname}@${host}:${root_dir}/${IP}${sub_dir}/interface${log_file}  ${local_dir}
fi


if [ ${log_type} = "activity" ]
then
        root_dir="/disk/ftp/YH_FTP/activity"
        sub_dir=""
        hosts_ip=""
echo   ${uname}@${host}:${root_dir}/${sub_dir}/${log_file}  ${local_dir}
 scp ${uname}@${host}:${root_dir}/${IP}${sub_dir}/${log_file}  ${local_dir}
fi
 if [ ${log_type} = "biz_provider_yongyou" ]
then
        rm -r ${src_logs}/${log_type}
        mkdir -p  ${local_dir}
        root_dir="/disk/ftp"
        sub_dir=""
        hosts_ip=""
        echo ${uname}@${host}:${root_dir}/YU_FTP/${sub_dir}/yongyou${log_file} ${local_dir}
        scp ${uname}@${host}:${root_dir}/YU_FTP/${sub_dir}/yongyou${log_file} ${local_dir}
fi
 if [ ${log_type} = "biz_provider_syd" ]
then
        rm -r ${src_logs}/${log_type}
        mkdir -p  ${local_dir}
        root_dir="/disk/ftp"
        sub_dir=""
        hosts_ip=""
        echo ${uname}@${host}:${root_dir}/SYD_FTP/${sub_dir}/syd${log_file} ${local_dir}
       scp ${uname}@${host}:${root_dir}/SYD_FTP/${sub_dir}/syd${log_file} ${local_dir}
fi
 if [ ${log_type} = "biz_provider_kxd" ]
then
        rm -r ${src_logs}/${log_type}
        mkdir -p  ${local_dir}
        root_dir="/disk/ftp"
        sub_dir=""
        hosts_ip=""
        echo ${uname}@${host}:${root_dir}/KXD_FTP/${sub_dir}/kxd${log_file} ${local_dir}
        scp ${uname}@${host}:${root_dir}/KXD_FTP/${sub_dir}/kxd${log_file} ${local_dir}
fi
 if [ ${log_type} = "kxd_pay" ]
then
        rm -r ${src_logs}/${log_type}
        mkdir -p  ${local_dir}
        root_dir="/disk/ftp"
        sub_dir=""
        hosts_ip=""
        echo ${uname}@${host}:${root_dir}/KXD_FTP/${sub_dir}/orderkxd${log_file} ${local_dir}
        scp ${uname}@${host}:${root_dir}/KXD_FTP/${sub_dir}/orderkxd${log_file} ${local_dir}
fi
 if [ ${log_type} = "syd_pay" ]
then
        rm -r ${src_logs}/${log_type}
        mkdir -p  ${local_dir}
        root_dir="/disk/ftp"
        sub_dir=""
        hosts_ip=""
        echo ${uname}@${host}:${root_dir}/SYD_FTP/${sub_dir}/ordersyd${log_file} ${local_dir}
        scp ${uname}@${host}:${root_dir}/SYD_FTP/${sub_dir}/ordersyd${log_file} ${local_dir}
fi
 if [ ${log_type} = "yl_pay" ]
then
        rm -r ${src_logs}/${log_type}
        mkdir -p  ${local_dir}
        root_dir="/disk/ftp"
        sub_dir="pay"
        hosts_ip=""
        echo ${uname}@${host}:${root_dir}/YL_FTP/${sub_dir}/${log_file}  ${local_dir}
        scp ${uname}@${host}:${root_dir}/YL_FTP/${sub_dir}/${log_file} ${local_dir}
fi
 if [ ${log_type} = "shop_broadband" ]
then
        rm -r ${src_logs}/${log_type}
        mkdir -p  ${local_dir}
        root_dir="/disk/ftp"
        sub_dir="shop"
        hosts_ip=""
        echo ${uname}@${host}:${root_dir}/YL_FTP/${sub_dir}/broadband${log_file}  ${local_dir}
        scp ${uname}@${host}:${root_dir}/YL_FTP/${sub_dir}/broadband${log_file} ${local_dir}
fi
 if [ ${log_type} = "shop_mobile" ]
then
        rm -r ${src_logs}/${log_type}
        mkdir -p  ${local_dir}
        root_dir="/disk/ftp"
        sub_dir="shop"
        hosts_ip=""
        echo ${uname}@${host}:${root_dir}/YL_FTP/${sub_dir}/mobile+${log_file}  ${local_dir}
        scp ${uname}@${host}:${root_dir}/YL_FTP/${sub_dir}/mobile+${log_file} ${local_dir}
fi
 if [ ${log_type} = "shop_tuidan" ]
then
        rm -r ${src_logs}/${log_type}
        mkdir -p  ${local_dir}
        root_dir="/disk/ftp"
        sub_dir="shop"
        hosts_ip=""
        echo ${uname}@${host}:${root_dir}/YL_FTP/${sub_dir}/mobiletuidan+${log_file}  ${local_dir}
        scp ${uname}@${host}:${root_dir}/YL_FTP/${sub_dir}/mobiletuidan+${log_file} ${local_dir}
fi 
#遍历各子目录
for IP in ${hosts_ip}
  do
	echo  ${uname}@${host}:${root_dir}/${IP}${sub_dir}/${log_file}
   	scp ${uname}@${host}:${root_dir}/${IP}${sub_dir}/${log_file} ${local_dir}
done

#统计文件信息
cd ${local_dir}
echo 'unzip files if zip'
for files in `ls *.*`
do
	if [ "${files##*.}" = "gz"  ]
	then
        	echo 'gunzip' $files
	        gunzip $files
        fi
done
f_count=`ls ${local_dir}/*${hour}*.log |wc -l`
rec_count=`cat ${local_dir}/*${hour}*.log |wc -l`
f_size=`du -hs`
#打印azkaban监控扫描信息
echo 'HIVE_TABLE_NAME:load-'$2'_'$1
echo 'Exported '$rec_count' records.'

run_type=''
rt=$3
if [[ $rt = "rerun" ]]
then
	run_type="手动执行"
else
	run_type="定时执行"
fi
etime=$(date +%s -d 'now')
echo "["${run_type}"] 总耗时:(秒)"$((($etime-$stime)))
echo "采集日志文件时间:[${hour}],采集类型:[${run_type}],总文件个数:[${f_count}],总记录数:[${rec_count}],总文件大小:["${f_size}"]" >> ${meta_logs}/${log_type}_${day}.meta

#$3='' 定时执行,$3=rerun 重新运行
#sh ${app_home}/analyzer/etl.sh ${hour} ${log_type} $3
fi
