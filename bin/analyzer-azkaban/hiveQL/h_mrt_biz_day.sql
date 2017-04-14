CREATE TEMPORARY FUNCTION str_to_date AS 'com.nexr.platform.hive.udf.UDFStrToDate';

-- 按天业务结果表
 INSERT OVERWRITE TABLE T_MRT_BIZERROR_DAY
  select  str_to_date(query_date,"yyyyMMdd"),
          pay_type_key,
         user_type_key,
          application_key,
          biz_type_key,
         biz_id,
         response_key,
         province_id,
         city_id,
          sum(cou),result_key
      from t_mid_all_biz_hour
    where result_key <> '1' and  dt >= '${hiveconf:dt}'
    and query_date = '${hiveconf:dt}'
      group by   str_to_date(query_date,"yyyyMMdd"),
          pay_type_key,
         user_type_key,
          application_key,
          biz_type_key,
         biz_id,
         response_key,
         province_id,
         city_id ,result_key;

INSERT OVERWRITE TABLE T_MRT_BIZRESULT_DAY
  select  str_to_date(query_date,"yyyyMMdd"),
           pay_type_key,
         user_type_key,
          application_key,
          biz_type_key,
         biz_id,
          province_id,
         city_id,
          sum(case when(result_key = '1') then cou else 0 end),
         sum(case when(result_key <> '1') then cou else 0 end)
      from t_mid_all_biz_hour
         where  dt >= '${hiveconf:dt}'
     and query_date = '${hiveconf:dt}'
         group by   str_to_date(query_date,"yyyyMMdd"),
          pay_type_key,
         user_type_key,
          application_key,
          biz_type_key,
         biz_id,
         province_id,
         city_id;


 INSERT OVERWRITE TABLE T_MRT_BIZTOTAL_HOUR
  select  str_to_date(query_date,"yyyyMMdd"),
         hour,
           pay_type_key,
         user_type_key,
          application_key,
          biz_type_key,
          province_id,
         city_id,
           sum(case when(result_key = '1') then cou else 0 end),
         sum(case when(result_key <> '1') then cou else 0 end)
      from t_mid_all_biz_hour
       where  dt >= '${hiveconf:dt}'
     and query_date = '${hiveconf:dt}'
           group by   str_to_date(query_date,"yyyyMMdd"),hour,
          pay_type_key,
         user_type_key,
          application_key,
          biz_type_key,
           province_id,
         city_id;

 
