set hive.exec.compress.output=true;
 -- 用户数中间表统计
 insert OVERWRITE  table t_mid_user PARTITION (dt='${hiveconf:dt}',chn='web') 
SELECT USER_MOBILE,
       substr(query_time, 0, 8),
       substr(query_time, 9, 2),
       PAY_ID,
       net_ID,
       location_TYPE,
       BRAND_ID,
       APPLICATION,
       VERSION_ID,
       BIZ_TYPE,
       new_BIZ_id,
       PROVINCE_ID,
       CITY_ID,
       count(1)
  FROM t_ods_biz_web
 where dt = '${hiveconf:dt}' 
   and result = '1'
 group by USER_MOBILE,
          substr(query_time, 0, 8),
          substr(query_time, 9, 2),
          PAY_ID,
          net_ID,
          location_TYPE,
          BRAND_ID,
          APPLICATION,
          VERSION_ID,
          BIZ_TYPE,
          new_BIZ_id,
          PROVINCE_ID,
          CITY_ID;
  

  insert OVERWRITE  table t_mid_user PARTITION (dt='${hiveconf:dt}',chn='mob') 
SELECT USER_MOBILE,
       substr(query_time, 0, 8),
       substr(query_time, 9, 2),
       PAY_ID,
       net_ID,
       location_TYPE,
       BRAND_ID,
       APPLICATION,
       VERSION_ID,
       BIZ_TYPE,
       new_BIZ_id,
       PROVINCE_ID,
       CITY_ID,
       count(1)
  FROM t_ods_biz_mob
 where dt = '${hiveconf:dt}' 
   and result = '1'
 group by USER_MOBILE,
          substr(query_time, 0, 8),
          substr(query_time, 9, 2),
          PAY_ID,
          net_ID,
          location_TYPE,
          BRAND_ID,
          APPLICATION,
          VERSION_ID,
          BIZ_TYPE,
          new_BIZ_id,
          PROVINCE_ID,
          CITY_ID;



insert OVERWRITE  table t_mid_user PARTITION (dt='${hiveconf:dt}',chn='sms') 
select * from (
SELECT USER_MOBILE,
       substr(query_time, 0, 8),
       substr(query_time, 9, 2),
       PAY_ID,
       net_ID,
       location_TYPE,
       BRAND_ID,
       APPLICATION,
       VERSION_ID,
       BIZ_TYPE,
       new_BIZ_id,
       PROVINCE_ID,
       CITY_ID,
       count(1)
  FROM t_ods_biz_sms
 where dt = '${hiveconf:dt}' 
 and fun_id<>'1'
 group by USER_MOBILE,
          substr(query_time, 0, 8),
          substr(query_time, 9, 2),
          PAY_ID,
          net_ID,
          location_TYPE,
          BRAND_ID,
          APPLICATION,
          VERSION_ID,
          BIZ_TYPE,
          new_BIZ_id,
          PROVINCE_ID,
          CITY_ID
union all 
 SELECT USER_MOBILE,
       substr(query_time, 0, 8),
       substr(query_time, 9, 2),
       PAY_ID,
       net_ID,
       location_TYPE,
       BRAND_ID,
       APPLICATION,
       VERSION_ID,
       BIZ_TYPE,
       new_BIZ_id,
       PROVINCE_ID,
       CITY_ID,
       count(1)
  FROM t_ods_biz_sms_sfts
 where dt = '${hiveconf:dt}' 
 group by USER_MOBILE,
          substr(query_time, 0, 8),
          substr(query_time, 9, 2),
          PAY_ID,
          net_ID,
          location_TYPE,
          BRAND_ID,
          APPLICATION,
          VERSION_ID,
          BIZ_TYPE,
          new_BIZ_id,
          PROVINCE_ID,
          CITY_ID) a;

insert OVERWRITE  table t_mid_user PARTITION (dt='${hiveconf:dt}',chn='smsfun') 
SELECT USER_MOBILE,
       substr(query_time, 0, 8),
       substr(query_time, 9, 2),
       PAY_ID,
       net_ID,
       location_TYPE,
       BRAND_ID,
       APPLICATION,
       VERSION_ID,
       BIZ_TYPE,
       new_BIZ_id,
       PROVINCE_ID,
       CITY_ID,
       count(1)
  FROM t_ods_biz_sms
 where dt = '${hiveconf:dt}'  
 and fun_id = '1'
 group by USER_MOBILE,
          substr(query_time, 0, 8),
          substr(query_time, 9, 2),
          PAY_ID,
          net_ID,
          location_TYPE,
          BRAND_ID,
          APPLICATION,
          VERSION_ID,
          BIZ_TYPE,
          new_BIZ_id,
          PROVINCE_ID,
          CITY_ID;



 -- 用户数结果表统计(按用户类型-日表 )  
 insert overwrite table t_mrt_user_usertype_date
  select t.province_id,
         t.city_id,
         t.query_date,
         t.application_key,
         t.user_type_key,
         t.pay_type_key,
         count(1) as cou
    from (select distinct user_mobile,
                          province_id,
                          city_id,
                          application_key,
                          user_type_key,
                          pay_type_key,
                          query_date
            from t_mid_user
           where-- query_date = '${hiveconf:dt}'	    and 
dt = '${hiveconf:dt}'
	    and chn <> 'smsfun') t
   group by t.province_id,
            t.city_id,
            t.application_key,
            t.user_type_key,
            t.pay_type_key,
            t.query_date;


 
-- 用户数结果表统计(按用户类型-月表 )  
 insert overwrite table t_mrt_user_usertype_mon
  select t.province_id,
         t.city_id,
         '${hiveconf:dt}',
         t.application_key,
         t.user_type_key,
         t.pay_type_key,
         count(1) as cou
    from (select distinct user_mobile,
                          province_id,
                          city_id,
                          application_key,
                          user_type_key,
                          pay_type_key
            from t_mid_user
           where dt >= '${hiveconf:sdt}' and dt <= '${hiveconf:dt}'   and chn <> 'smsfun') t
   group by t.province_id,
            t.city_id,
            t.application_key,
            t.user_type_key,
            t.pay_type_key;



-- 用户数结果表统计(按用户类型-3月表 )
 insert overwrite table t_mrt_user_usertype_3mon
  select t.province_id,
         t.city_id,
         '${hiveconf:dt}',
         t.application_key,
         t.user_type_key,
         t.pay_type_key,
         count(1) as cou
    from (select distinct user_mobile,
                          province_id,
                          city_id,
                          application_key,
                          user_type_key,
                          pay_type_key
            from t_mid_user
           where dt >= '${hiveconf:m3dt}' and dt <= '${hiveconf:dt}'   and chn <> 'smsfun' ) t
   group by t.province_id,
            t.city_id,
            t.application_key,
            t.user_type_key,
            t.pay_type_key;