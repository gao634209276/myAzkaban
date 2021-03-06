select trans_id,
       user_mobile,
       province_id,
       city_id,
       net_id,
       pay_id,
       brand_id,
       provider_key,
       application,
       version,
       interface_id,
       interface_name,
       interface_type,
       result,
       fail_reson,
       response_code,
       query_time,
       change_type,
       cur_package_id,
       cur_package_name,
       cur_package_type,
       new_package_id,
       new_package_name,
       new_packaget_type,
       new_product_type,
       operate_type,
       commit_type,
       is_reback,
       effect_type,
       user_ip,
       location,
       wi,
       sourceid
  from t_ods_biz_intf_v3
  where dt='${hiveconf:strdate}' and application like '1130%';

CREATE TEMPORARY FUNCTION decode AS 'com.nexr.platform.hive.udf.GenericUDFDecode';
select bss_id,
       user_mobile,
       province_id,
       city_id,
       net_id,
       pay_id,
       brand_id,
       provider_key,
       application,
       version_id,
       substr(new_biz_id, 8, 4),
       '',
       decode(biz_type, '2', '3', biz_type),
       result,
       fail_reson,
       response_code,
       query_time,
       '',
       cur_product_id,
       '',
       '',
       new_product_id,
       '',
       '',
       '',
       operate_type,
       is_precommit,
       is_reback,
       effect_type,
       user_ip,
       location_type,
       wi,
       sourceid
  from t_ods_biz_mob
 where dt = '${hiveconf:strdate}';


