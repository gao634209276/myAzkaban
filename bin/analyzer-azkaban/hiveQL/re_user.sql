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


set hive.exec.compress.output=false;

 -- 登录用户数结果表统计(按用户类型-日表 )  
 insert overwrite table T_MRT_LOGIN_USER_DATE
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
           where query_date = '${hiveconf:dt}'
	    and dt >= '${hiveconf:dt}' and chn='login') t
   group by t.province_id,
            t.city_id,
            t.application_key,
            t.user_type_key,
            t.pay_type_key,
            t.query_date;



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


 
