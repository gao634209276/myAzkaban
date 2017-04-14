insert  overwrite table t_ods_tmpmobbiz
  select bss_id,
         user_mobile,
         query_time,
         pay_id,
         net_id,
         location_type,
         brand_id,
         application,
         version_id,
         new_biz_id,
         province_id,
         city_id,
         result,
         operate_type,
         cur_product_id,
         new_product_id,
         effect_type,
         insert_time,
         dt
    from t_ods_biz_mob
    where dt>='${hiveconf:dt}'
	and substr(query_time,0,8)='${hiveconf:dt}'
    and biz_type='2';
    
  insert  into table t_ods_tmpmobbiz
  select bss_id,
         user_mobile,
         query_time,
         pay_id,
         net_id,
         location_type,
         brand_id,
         application,
         version_id,
         new_biz_id,
         province_id,
         city_id,
         result,
         operate_type,
         cur_product_id,
         new_product_id,
         effect_type,
         insert_time,
         dt
    from t_ods_biz_web
     where dt>='${hiveconf:dt}'
	 and substr(query_time,0,8)='${hiveconf:dt}'
    and biz_type='2';
   
    insert  into table t_ods_tmpmobbiz
  select bss_id,
         user_mobile,
         query_time,
         pay_id,
         net_id,
         location_type,
         brand_id,
         application,
         version_id,
         new_biz_id,
         province_id,
         city_id,
         result,
         operate_type,
         cur_product_id,
         new_product_id,
         effect_type,
         insert_time,
         dt
    from t_ods_biz_sms
     where dt>='${hiveconf:dt}'
	 and substr(query_time,0,8)='${hiveconf:dt}'
    and biz_type='2';

    insert  into table t_ods_tmpmobbiz
  select order_id,
         user_mobile,
         create_time,
         pay_id,
         net_id,
         'yl',
         '',
         application,
         '',
         new_biz_id,
         province_id,
         city_id,
         result,
         operate_type,
         cur_product_id,
         new_product_id,
         effect_type,
         insert_time,
         dt
    from t_ods_biz_auto
      where dt>='${hiveconf:dt}'
	  and substr(create_time,0,8)='${hiveconf:dt}'
    and biz_type='3'; 
    
