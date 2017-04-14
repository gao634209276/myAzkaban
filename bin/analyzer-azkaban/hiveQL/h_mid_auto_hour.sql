  CREATE TEMPORARY FUNCTION str_to_date AS 'com.nexr.platform.hive.udf.UDFStrToDate';
  CREATE TEMPORARY FUNCTION decode AS 'com.nexr.platform.hive.udf.GenericUDFDecode';

  set hive.exec.compress.output=true;
 -- h_mid_auto_hour.sql自助服务业务-统计表_分小时
 insert into table t_mid_biz_auto_hour partition(dt='${hiveconf:dt}') 
select substr(create_time, 0, 8),
       substr(create_time, 9, 2),
       application,
       decode(biz_type, 3, 2, 2, 3, biz_type),
       substr(new_biz_id, 8, 4),
       net_id,
       pay_id,
       province_id,
       city_id,
       result,
       interface_id,
       interface_remark,
       fail_bssid,
       combo_key,
       operate_type,
       cur_product_id,
       new_product_id,
       effect_type,
       count(1),
       insert_time
  from t_ods_biz_auto
 where dt = '${hiveconf:dt}'
   and insert_time > '${hiveconf :hour}'
 group by substr(create_time, 0, 8),
          substr(create_time, 9, 2),
          application,
          decode(biz_type, 3, 2, 2, 3, biz_type),
          substr(new_biz_id, 8, 4),
          net_id,
          pay_id,
          province_id,
          city_id,
          result,
          interface_id,
          interface_remark,
          fail_bssid,
          combo_key,
          operate_type,
          cur_product_id,
          new_product_id,
          effect_type,
          insert_time;
