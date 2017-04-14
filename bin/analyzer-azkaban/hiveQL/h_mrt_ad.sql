 CREATE TEMPORARY FUNCTION str_to_date AS 'com.nexr.platform.hive.udf.UDFStrToDate';

 -- 统一广告结果表
insert overwrite table t_mid_ad   
 select
 t.channel_no as APPLICATION_KEY,
 t.ad_name as AD_NAME,
 t.ad_pos_name as ADING_NAME,
 str_to_date(substr(query_time,0,8),"yyyyMMdd"),
 substr(query_time, 9, 2),
 t.prov_id as PROVINCE_ID,
 t.city_id as CITY_ID,
 count(*) as COU
  from t_ods_ad t 
  where  substr(query_time,0,8)='${hiveconf:dt}'
     and dt >= '${hiveconf:dt}'
 group by t.channel_no,
          t.ad_name,
          t.ad_pos_name,
          str_to_date(substr(query_time,0,8),"yyyyMMdd"),
 substr(query_time, 9, 2),
          t.prov_id,
          t.city_id

