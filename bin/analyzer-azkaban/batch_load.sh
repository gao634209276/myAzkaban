#--------------- 
#!/bin/sh 

  echo   "******************************************************"
  echo   "*       日志数据分析批量执行  by yuanyingsheng       *"
  echo   "******************************************************" 


stime=$(date +%s -d 'now')

echo " 执行数据抽取并导入hadoop..."

execByDay(){
        date1=$1
        date2=$2
	log_type=$3
        table_name=$4
        if [ $# = 1 ]
        then
          date2=$1
        fi
        datestring=""
        st=`date -d "$date1" +%s`
        et=`date -d "$date2" +%s`
        while [ "$st" -le "$et" ]
        do
          datestring=`date -d @$st +%F`
          dstr=`date -d "$datestring" +"%Y%m%d"`
		      echo $dstr
		     # sh /home/sinova/bin/analyzer/load.sh $dstr $log_type rerun
          	sh /home/sinova/bin/analyzer-azkaban/load_gz.sh $dstr $log_type rerun
		st=$((st+86400))
        done
}

execByDay $1 $2 $3 $4

echo "数据导入执行完毕！导入日期:$1~$2"
etime=$(date +%s -d 'now')
echo "总消耗时间:(分钟)"$((($etime-$stime)/60))
 
#-------------- 
