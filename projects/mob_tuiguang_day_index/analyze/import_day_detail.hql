set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.compress.output=true;
set  hive.auto.convert.join=false;
--基础数据处理 每日插入
--手厅登录明细 与 推荐表 交集
 
  insert overwrite table t_mid_mob_log partition
  (dt)
   select distinct mobileno mob, mon
   from t_user_success
  where mon  = '${dayAgo}'
    and application in ('wap', 'standard', 'iphone_c', 'android');
    
 
   
 --接口明细
 insert overwrite table t_mid_tg_f_mob_intf partition
  (dt)
  select t1.*
    from (select bss_id,
                 user_mobile,
                 province_id,
                 city_id,
                 net_id,
                 interface_id,
                 interface_name,
                 interface_type,
                 result,
                 fail_reson,
                 response_code,
                 query_time,
                 change_type,
                 new_package_id,
                 new_package_name,
                 new_package_type,
                 new_product_type,
                 operate_type,
                 sourceid,
                 application,
                 dt
            from t_ods_3hall_intf
           where application like '113%' and operate_type <> '1'
          and result = '1' and substr(query_time, 0, 8) ='${dayAgo}'
          and INTERFACE_TYPE = '3' and dt>='${dayAgo}') t1
    join (select distinct user_mobile_num from t_tui_guang_client where dt='${dayAgo}') t2 on t1.user_mobile = t2.user_mobile_num;
    
-----流量包明细 
insert overwrite table t_mid_mob_llb
partition (dt)
  select bss_id,
         user_mobile,
         province_id,
         city_id,
         interface_id,
         interface_name,
         interface_type,
         result,
         fail_reson,
         response_code,
         query_time,
         operate_type,
         sourceid,
         new_package_id,
         dt
    from t_mid_tg_f_mob_intf
   where interface_id in ('0175','0252','0207','0205','0209','0211','0264')
   and dt='${dayAgo}'
     and new_package_id not in
         ('88122857', '88122855', '88122856', '88122854', '50031951');
		 

---------------------------------------------中间表------------------------------------------------		
---------------------------------------------------------------------------------------------------------		 
--推荐表左关联 列出手厅登录明细
insert overwrite table t_tmp_f_mob  
     select t1.mob,
            t2.dt,
            t1.dt_tg
        from (select user_mobile_num mob,
                    max(substr(regexp_replace(register_time, '-', ''), 0, 8)) dt_tg
               from t_tui_guang_client where dt='${dayAgo}' 
                and substr(regexp_replace(register_time, '-', ''), 0, 6) =substr('${dayAgo}',0,6)
              group by user_mobile_num) t1
       left join t_mid_mob_log t2 on t1.mob = t2.mob;
 
				   
 
