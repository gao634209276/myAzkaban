CREATE TEMPORARY FUNCTION str_to_date AS 'com.nexr.platform.hive.udf.UDFStrToDate';
set hive.exec.compress.output=true;
CREATE TEMPORARY FUNCTION substr AS 'com.nexr.platform.hive.udf.UDFSubstrForOracle';
-- h_mrt_biz_hour.sql 业务统计表_分小时
 INSERT into TABLE T_MID_BIZ_HOUR PARTITION(dt='${hiveconf:dt}',chn='web') 
select substr(query_time, 0, 8),
       substr(query_time, 9, 2),
       pay_id,
       net_id,
       location_type,
       brand_id,
       application,
       version_id,
       biz_type,
       substring(new_biz_id,-4,4),
       is_precommit,
       response_code,
       result,
       province_id,
       city_id,
       count(1),'','','',insert_time
  from t_ods_biz_web
 where   dt='${hiveconf:dt}'
    and insert_time > ${hiveconf:hour}
  group by substr(query_time, 0, 8),
       substr(query_time, 9, 2),
       pay_id,
       net_id,
       location_type,
       brand_id,
       application,
       version_id,
       biz_type,
       substring(new_biz_id,-4,4),
       is_precommit,
       response_code,
       result,
       province_id,
       city_id,insert_time;

        INSERT into TABLE T_MID_BIZ_HOUR PARTITION(dt='${hiveconf:dt}',chn='mob') 
select substr(query_time, 0, 8),
       substr(query_time, 9, 2),
       pay_id,
       net_id,
       location_type,
       brand_id,
       application,
       version_id,
       biz_type,
       substring(new_biz_id,-4,4),
       is_precommit,
       response_code,
       result,
       province_id,
       city_id,
       count(1),'','','',insert_time
  from t_ods_biz_mob
 where  dt='${hiveconf:dt}'
   and insert_time > ${hiveconf:hour}
  group by substr(query_time, 0, 8),
       substr(query_time, 9, 2),
       pay_id,
       net_id,
       location_type,
       brand_id,
       application,
       version_id,
       biz_type,
       substring(new_biz_id,-4,4),
       is_precommit,
       response_code,
       result,
       province_id,
       city_id,insert_time;

        INSERT into TABLE T_MID_BIZ_HOUR PARTITION(dt='${hiveconf:dt}',chn='sms') 
select substr(query_time, 0, 8),
       substr(query_time, 9, 2),
       pay_id,
       net_id,
       location_type,
       brand_id,
       application,
       version_id,
       biz_type,
       substring(new_biz_id,-4,4),
       is_precommit,
       response_code,
       result,
       province_id,
       city_id,
       count(1),fun_id,functiontype,servicecode,insert_time
  from t_ods_biz_sms
 where  dt='${hiveconf:dt}' 
   and insert_time > ${hiveconf:hour}
  group by substr(query_time, 0, 8),
       substr(query_time, 9, 2),
       pay_id,
       net_id,
       location_type,
       brand_id,
       application,
       version_id,
       biz_type,
       substring(new_biz_id,-4,4),
       is_precommit,
       response_code,
       result,
       province_id,
       city_id,fun_id,functiontype,servicecode,insert_time;


     INSERT  into TABLE T_MID_BIZ_HOUR PARTITION(dt='${hiveconf:dt}',chn='sms') 
select substr(query_time, 0, 8),
       substr(query_time, 9, 2),
       pay_id,
       net_id,
       location_type,
       brand_id,
       application,
       version_id,
       biz_type,
       new_biz_id,
       '',
       fail_reson,
       result,
       province_id,
       city_id,
       count(1),'','','',insert_time
  from t_ods_biz_sms_sfts
 where  dt='${hiveconf:dt}' 
   and insert_time > ${hiveconf:hour}
  group by substr(query_time, 0, 8),
       substr(query_time, 9, 2),
       pay_id,
       net_id,
       location_type,
       brand_id,
       application,
       version_id,
       biz_type,
       new_biz_id,
       fail_reson,
       result,
       province_id,
       city_id,insert_time;

set hive.exec.compress.output=false;
-- 按小时失败统计
INSERT OVERWRITE TABLE T_MRT_BIZERROR_HOUR
  select  str_to_date(query_date,"yyyyMMdd"),
          hour,
          pay_type_key,
         user_type_key,
          application_key,
          biz_type_key,
         biz_id,
         response_key,
         province_id,
         city_id,
          sum(cou),result_key
      from T_MID_BIZ_HOUR
    where result_key <> '1' and  dt='${hiveconf:dt}'
    and insert_time > ${hiveconf:hour}
      group by   str_to_date(query_date,"yyyyMMdd"),
          pay_type_key,
         user_type_key,
          application_key,
          biz_type_key,
         biz_id,
         response_key,
         province_id,
         city_id ,result_key,hour;
-- 按小时业务统计
INSERT OVERWRITE TABLE T_MRT_BIZRESULT_HOUR
  select  str_to_date(query_date,"yyyyMMdd"),
          hour,
          pay_type_key,
         user_type_key,
          application_key,
          biz_type_key,
         biz_id,
         province_id,
         city_id,
	  sum(case when(result_key = '1') then cou else 0 end),
          sum(case when(result_key <> '1') then cou else 0 end)
       from T_MID_BIZ_HOUR
    where   dt='${hiveconf:dt}'
    and insert_time > ${hiveconf:hour}
      group by   str_to_date(query_date,"yyyyMMdd"),
          pay_type_key,
         user_type_key,
          application_key,
          biz_type_key,
         biz_id,
         province_id,
         city_id ,hour;
