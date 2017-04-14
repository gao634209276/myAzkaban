#!/bin/bash
###########################################################################################################
### 作用:此脚本包含公用方法                                                                            ####
###########################################################################################################


 function getDate_str()
{
  #########################################################################################################
  ## 作用:传入参数，返回指定日期                      						       ####
  ## 参数:$1 $2          									       ####
  ##      $1 必填  hour_ago|day_ago|mon_ago|mon3_ago 						       ####
  ##      $2 非必填 指定日期									       ####
  ## 返回  当$2为空时返回上一小时，上一天，上月，近三月                                                ####
  ##       当$2非空时返回指定日期的 上一小时，上一天，上月，近三月                                     ####
  ######################################################################################################### 

  day=''    #返回日期
  date_type=''
  cur_day=''
  len=`expr length $1`
  len=`echo $1|awk '{print length($0)}'`
 if [ $# -lt 1  ]
  then
         echo "Usage: date_util.sh [hour_ago|day_ago|mon_ago|mon3_ago] [yyyymmdd|yyyymmddhh]"
  else
  if [ $# -eq 2 ] 
   then
          date_type=$1
          cur_day=$2
  elif [ $# -eq 1 ]
    then  
       ##不传第二个传数时默认当前小时
        date_type=$1  
        cur_date=`date +%Y%m%d%H` 
   fi
 
 if [[ $date_type  = "hour_ago"  ]] 
   then
       #返回上一小时
	day=`date -d"$cur_day 1 hour ago" +%Y%m%d%H`
  elif [[ $date_type = "day_ago" ]] 
  then
        #返回前一天日期
	day=`date -d"$cur_day 1 day ago" +%Y%m%d`
  elif [[ $date_type = "mon_ago" ]]
  then
        #返回上个月
        day=`date -d "$cur_day 1 months ago" +%Y%m%d`
   elif [[ $date_type = "mon3_ago" ]]
  then
        #返回近三月
        day=`date -d "$cur_day -2 months" +%Y%m%d`  
  else
        #手动输入 执行日期
        day=$1

  fi
  echo $day
fi
}


str=$(getDate_str $1 $2)
echo "$str"

