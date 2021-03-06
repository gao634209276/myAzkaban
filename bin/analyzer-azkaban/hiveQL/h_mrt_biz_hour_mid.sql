CREATE TEMPORARY FUNCTION str_to_date AS 'com.nexr.platform.hive.udf.UDFStrToDate';
set hive.exec.compress.output=false;
CREATE TABLE if not exists `T_MID_ALL_BIZ_HOUR`(
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
  `is_precommit` string, 
  `response_key` string, 
  `result_key` string, 
  `province_id` string, 
  `city_id` string, 
  `cou` int, 
  `fun_id` string, 
  `functiontype` string, 
  `servicecode` string, 
  `insert_time` string)
PARTITIONED BY (`dt` string) STORED AS ORC;

-- 聚合三厅业务量插入中间表
INSERT INTO TABLE T_MID_ALL_BIZ_HOUR PARTITION(dt='${hiveconf:dt}') 
SELECT substr(query_time, 0, 8),
       substr(query_time, 9, 2),
       pay_id,
       net_id,
       location_type,
       brand_id,
       application,
       version_id,
       biz_type,
       substring(new_biz_id,-4,4) new_biz_id,
       is_precommit,
       response_code,
       result,
       province_id,
       city_id,
       count(1),'' fun_id,'' functiontype,'' servicecode,insert_time 
FROM t_ods_biz_web 
WHERE dt='${hiveconf:dt}' AND insert_time > ${hiveconf:hour} 
GROUP BY substr(query_time, 0, 8),
       substr(query_time, 9, 2),
       pay_id,
       net_id,
       location_type,
       brand_id,
       application,
       version_id,
       biz_type,
       substring(new_biz_id,-4,4),
       is_precommit,
       response_code,
       result,
       province_id,
       city_id,insert_time 
UNION ALL 
SELECT substr(query_time, 0, 8),
       substr(query_time, 9, 2),
       pay_id,
       net_id,
       location_type,
       brand_id,
       application,
       version_id,
       biz_type,
       substring(new_biz_id,-4,4) new_biz_id,
       is_precommit,
       response_code,
       result,
       province_id,
       city_id,
       count(1),'' fun_id,'' functiontype,'' servicecode,insert_time 
FROM t_ods_biz_mob 
WHERE dt='${hiveconf:dt}' AND insert_time > ${hiveconf:hour} 
GROUP BY substr(query_time, 0, 8),
       substr(query_time, 9, 2),
       pay_id,
       net_id,
       location_type,
       brand_id,
       application,
       version_id,
       biz_type,
       substring(new_biz_id,-4,4),
       is_precommit,
       response_code,
       result,
       province_id,
       city_id,insert_time 
UNION ALL 
SELECT substr(query_time, 0, 8),
       substr(query_time, 9, 2),
       pay_id,
       net_id,
       location_type,
       brand_id,
       application,
       version_id,
       biz_type,
       new_biz_id,
       '' is_precommit,
       fail_reson response_code,
       result,
       province_id,
       city_id,
       count(1),'' fun_id,'' functiontype,'' servicecode,insert_time 
FROM t_ods_biz_sms_sfts 
WHERE dt='${hiveconf:dt}' AND insert_time > ${hiveconf:hour} 
GROUP BY substr(query_time, 0, 8),
       substr(query_time, 9, 2),
       pay_id,
       net_id,
       location_type,
       brand_id,
       application,
       version_id,
       biz_type,
       new_biz_id,
       fail_reson,
       result,
       province_id,
       city_id,insert_time 
UNION ALL 
SELECT substr(query_time, 0, 8),
       substr(query_time, 9, 2),
       pay_id,
       net_id,
       location_type,
       brand_id,
       application,
       version_id,
       biz_type,
       substring(new_biz_id,-4,4) new_biz_id,
       is_precommit,
       response_code,
       result,
       province_id,
       city_id,
       count(1),fun_id,functiontype,servicecode,insert_time 
FROM t_ods_biz_sms 
WHERE dt='${hiveconf:dt}' AND insert_time > ${hiveconf:hour} 
GROUP BY substr(query_time, 0, 8),
       substr(query_time, 9, 2),
       pay_id,
       net_id,
       location_type,
       brand_id,
       application,
       version_id,
       biz_type,
       substring(new_biz_id,-4,4), 
       is_precommit,
       response_code,
       result,
       province_id,
       city_id,fun_id,functiontype,servicecode,insert_time;
