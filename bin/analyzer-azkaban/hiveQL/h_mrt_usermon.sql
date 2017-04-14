CREATE TABLE IF NOT EXISTS `t_mrt_login_user_mon`(
  `province_id` string, 
  `city_id` string, 
  `query_time` string, 
  `application_key` string, 
  `user_type_key` string, 
  `pay_type_key` string, 
  `cou` int);

CREATE TABLE IF NOT EXISTS `t_mrt_login_user_3mon`(
  `province_id` string, 
  `city_id` string, 
  `query_time` string, 
  `application_key` string, 
  `user_type_key` string, 
  `pay_type_key` string, 
  `cou` int);

CREATE TABLE IF NOT EXISTS `t_mrt_user_usertype_mon`(
  `province_id` string, 
  `city_id` string, 
  `query_time` string, 
  `application_key` string, 
  `user_type_key` string, 
  `pay_type_key` string, 
  `biz_type_key` string, 
  `cou` bigint);

CREATE TABLE IF NOT EXISTS `t_mrt_user_usertype_3mon`(
  `province_id` string, 
  `city_id` string, 
  `query_time` string, 
  `application_key` string, 
  `user_type_key` string, 
  `pay_type_key` string, 
  `cou` bigint);

INSERT OVERWRITE TABLE t_mrt_login_user_mon
SELECT province_id,
    city_id,
    '${hiveconf:dt}',
    application_key,
    user_type_key,
    pay_type_key,
    COUNT(DISTINCT user_mobile) cou
FROM t_mid_user
WHERE dt >= '${hiveconf:sdt}' AND dt <= '${hiveconf:dt}' AND chn='login'
GROUP BY province_id,
    city_id,
    application_key,
    user_type_key,
    pay_type_key,
    query_date;

INSERT OVERWRITE TABLE t_mrt_login_user_3mon
SELECT province_id,
    city_id,
    '${hiveconf:dt}',
    application_key,
    user_type_key,
    pay_type_key,
    COUNT(DISTINCT user_mobile) cou
FROM t_mid_user
WHERE dt >= '${hiveconf:m3dt}' AND dt <= '${hiveconf:dt}' AND chn='login'
GROUP BY province_id,
    city_id,
    application_key,
    user_type_key,
    pay_type_key,
    query_date;

INSERT OVERWRITE TABLE t_mrt_user_usertype_mon
SELECT province_id,
    city_id,
    '${hiveconf:dt}',
    application_key,
    user_type_key,
    pay_type_key,
    count(distinct user_mobile) cou 
    FROM t_mid_user
    WHERE dt >= '${hiveconf:sdt}' AND dt <= '${hiveconf:dt}' AND chn <> 'smsfun'
GROUP BY province_id,
    city_id,
    application_key,
    user_type_key,
    pay_type_key, 
    query_date;

INSERT OVERWRITE TABLE t_mrt_user_usertype_3mon
SELECT province_id,
    city_id,
    '${hiveconf:dt}',
    application_key,
    user_type_key,
    pay_type_key,
    count(distinct user_mobile) cou 
    FROM t_mid_user
    WHERE dt >= '${hiveconf:m3dt}' AND dt <= '${hiveconf:dt}' AND chn <> 'smsfun'
GROUP BY province_id,
    city_id,
    application_key,
    user_type_key,
    pay_type_key,
    query_date;
