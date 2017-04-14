CREATE TEMPORARY FUNCTION str_to_date AS 'com.nexr.platform.hive.udf.UDFStrToDate';
set hive.exec.compress.output=false;
FROM t_mid_all_biz_hour 
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
         city_id ,result_key,hour 
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
