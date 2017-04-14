set hive.exec.compress.output=true;
 -- 用户数中间表统计
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

--登录 用户数结果表统计(按用户类型-月表 )  
 insert overwrite table T_MRT_LOGIN_USER_MON
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
           where query_date >= '${hiveconf:sdt}' and query_date <= '${hiveconf:dt}'  and chn='login') t
   group by t.province_id,
            t.city_id,
            t.application_key,
            t.user_type_key,
            t.pay_type_key;

-- 登录 用户数结果表统计(按用户类型-3月表 )
 insert overwrite table T_MRT_LOGIN_USER_3MON
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
           where query_date >= '${hiveconf:m3dt}' and query_date <= '${hiveconf:dt}'  and chn='login') t
   group by t.province_id,
            t.city_id,
            t.application_key,
            t.user_type_key,
            t.pay_type_key;
