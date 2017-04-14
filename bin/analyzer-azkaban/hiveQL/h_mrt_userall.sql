

CREATE TABLE IF NOT EXISTS `t_mrt_user_usertype_date`(
  `province_id` string, 
  `city_id` string, 
  `time_key` bigint, 
  `application_key` string, 
  `user_type_key` string, 
  `pay_type_key` string, 
  `cou` bigint);

CREATE TABLE IF NOT EXISTS `T_MRT_LOGIN_USER_DATE`(
  `province_id` string, 
  `city_id` string, 
  `query_time` string, 
  `application_key` string, 
  `user_type_key` string, 
  `pay_type_key` string, 
  `cou` int);

 -- 用户数结果表(登录用户数，使用用户数:分日，月，3月 )  
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
           where dt = '${hiveconf:dt}'
	    and chn <> 'smsfun') t
   group by t.province_id,
            t.city_id,
            t.application_key,
            t.user_type_key,
            t.pay_type_key,
            t.query_date;

