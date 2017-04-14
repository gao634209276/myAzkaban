set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict; 

insert overwrite  table t_ods_3hall_to_zhfw partition (dt='${hiveconf:dt}')
select d1.key,
       d1.bss_id,
       d1.user_mobile,
       d1.province_id,
       d1.biz_code,
       d1.application,
       d1.biz_time,
       d1.biz_result
from
(select concat(reverse(user_mobile),biz_code) key,
       bss_id,
       user_mobile,
       province_id,
       biz_code,
       application,
       biz_time,
       biz_result,
       row_number() over(partition by user_mobile,biz_code order by biz_time desc) as number
from t_ods_3hall_biz where dt<=from_unixtime(unix_timestamp(),'yyyyMM') and dt>='${hiveconf:sdt}'
)d1
left join 
(select biz_id from T_BIZ_BASE where biz_type=02
)d2
on(d1.biz_code=d2.biz_id)
where d1.number=1 ;

