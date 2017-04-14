set hive.exec.compress.output=false;


CREATE TABLE IF NOT EXISTS `t_mid_user`(
  `user_mobile` string, 
  `query_date` string, 
  `hour` int, 
  `pay_type_key` string, 
  `user_type_key` string, 
  `login_type_key` string, 
  `brand_key` string, 
  `application_key` string, 
  `version_id` string, 
  `biz_type_key` string, 
  `biz_id` string, 
  `province_id` string, 
  `city_id` string, 
  `suc_cou` int)
PARTITIONED BY ( 
  `dt` string, 
  `chn` string);



 -- h_mid_user.sql 用户数中间表统计
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

 insert OVERWRITE  table t_mid_user PARTITION (dt='${hiveconf:dt}' ,chn='login')
SELECT USER_MOBILE,
       substr(query_time, 0, 8),
       substr(query_time, 9, 2),
       t.pay_type_key,
       t.user_type_key,
       t.login_type_key,
       t.brand_key,
       case
         when t.application_key = '113000004' then
          '113000004'
         when application_key = '113000005' then
          '113000005'
         else
          '111000002'
       end,
       '',
       '2',
       '1001',
       concat('0',PROVINCE_ID),
       CITY_ID,
       count(1)
  FROM t_ods_login t
 where result_key = '1'
 and dt = '${hiveconf:dt}' 
 group by USER_MOBILE,
          substr(query_time, 0, 8),
          substr(query_time, 9, 2),
          t.pay_type_key,
          t.user_type_key,
          t.login_type_key,
          t.brand_key,
          case
            when t.application_key = '113000004' then
             '113000004'
            when application_key = '113000005' then
             '113000005'
            else
             '111000002'
          end,
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
