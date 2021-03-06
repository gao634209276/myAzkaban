select bss_id,
       user_mobile,
       query_time,
       pay_id,
       net_id,
       location_type,
       brand_id,
       application,
       version_id,
       biz_type,
       new_biz_id,
       is_precommit,
       response_code,
       result,
       provider_key,
       province_id,
       city_id,
       user_ip,
       nodepoint_name,
       combo_key,
       operate_type,
       cur_product_id,
       new_product_id,
       operator_id,
       is_reback,
       effect_type,
       bss_product_id,
       father_id,
       user_type,
       product_type,
       product_sounce,
       product_sign,
       begin_time,
       isleaf,
       iscommend,
       isaptitude,
       areaid,
       fun_id,
       functiontype,
       servicecode,
       servicename,
       responsetime,
       usetime,
       openorclose,
       regexp_replace(substr(reqmes, 0, 3000), '\\s+', ''),
       regexp_replace(substr(resmes, 0, 3000), '\\s+', ''),
       query_mobileno,
       query_servicecode,
       requestprotime,
       responseprotime,
       biflag,
       newreqmes,
       listflag,
       '',
       ''
  from t_ods_f_biz_sms
 where dt = '${hiveconf:strdate}';

