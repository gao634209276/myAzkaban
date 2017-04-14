--检查创建表结构
CREATE TABLE IF NOT EXISTS `T_MRT_BIZERROR_DAY`(
  `query_date` string, 
  `pay_type_key` string, 
  `user_type_key` string, 
  `application_key` string, 
  `biz_type_key` string, 
  `biz_id` string, 
  `response_key` string, 
  `province_id` string, 
  `city_id` string, 
  `fail_cou` int, 
  `result_key` string);

CREATE TABLE IF NOT EXISTS `T_MRT_BIZRESULT_DAY`(
  `query_date` string, 
  `pay_type_key` string, 
  `user_type_key` string, 
  `application_key` string, 
  `biz_type_key` string, 
  `biz_id` string, 
  `province_id` string, 
  `city_id` string, 
  `suc_cou` int, 
  `fail_cou` int);

CREATE TABLE IF NOT EXISTS `T_MRT_BIZTOTAL_HOUR`(
  `query_date` string, 
  `hour` int, 
  `pay_type_key` string, 
  `user_type_key` string, 
  `application_key` string, 
  `biz_type_key` string, 
  `province_id` string, 
  `city_id` string, 
  `suc_cou` int, 
  `fail_cou` int);
--构建自定义函数
CREATE TEMPORARY FUNCTION str_to_date AS 'com.nexr.platform.hive.udf.UDFStrToDate';
-- 一次扫描合并计算
FROM t_mid_all_biz_hour 
INSERT OVERWRITE TABLE T_MRT_BIZERROR_DAY 
SELECT str_to_date(query_date,"yyyyMMdd"),
    pay_type_key,
    user_type_key,
    application_key,
    biz_type_key,
    biz_id,
    response_key,
    province_id,
    city_id,
    sum(cou),
    result_key 
WHERE result_key <> '1' 
    AND  dt >= '${hiveconf:dt}' 
    AND query_date = '${hiveconf:dt}' 
GROUP BY str_to_date(query_date,"yyyyMMdd"),
    pay_type_key,
    user_type_key,
    application_key,
    biz_type_key,
    biz_id,
    response_key,
    province_id,
    city_id ,
    result_key 
INSERT OVERWRITE TABLE T_MRT_BIZRESULT_DAY 
SELECT str_to_date(query_date,"yyyyMMdd"),
    pay_type_key,
    user_type_key,
    application_key,
    biz_type_key,
    biz_id,
    province_id,
    city_id,
    sum(case when(result_key = '1') then cou else 0 end),
    sum(case when(result_key <> '1') then cou else 0 end) 
WHERE dt >= '${hiveconf:dt}' 
    AND query_date = '${hiveconf:dt}' 
GROUP BY str_to_date(query_date,"yyyyMMdd"),
    pay_type_key,
    user_type_key,
    application_key,
    biz_type_key,
    biz_id,
    province_id,
    city_id 
INSERT OVERWRITE TABLE T_MRT_BIZTOTAL_HOUR 
SELECT str_to_date(query_date,"yyyyMMdd"),
    hour,
    pay_type_key,
    user_type_key,
    application_key,
    biz_type_key,
    province_id,
    city_id,
    sum(case when(result_key = '1') then cou else 0 end),
    sum(case when(result_key <> '1') then cou else 0 end) 
WHERE dt >= '${hiveconf:dt}' 
    AND query_date = '${hiveconf:dt}' 
GROUP BY str_to_date(query_date,"yyyyMMdd"),hour,
    pay_type_key,
    user_type_key,
    application_key,
    biz_type_key,
    province_id,
    city_id;
