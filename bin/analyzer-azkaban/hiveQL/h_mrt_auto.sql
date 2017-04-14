CREATE TEMPORARY FUNCTION str_to_date AS 'com.nexr.platform.hive.udf.UDFStrToDate';
CREATE TEMPORARY FUNCTION decode AS 'com.nexr.platform.hive.udf.GenericUDFDecode';

  set hive.exec.compress.output=true;
 
 -- 自助服务按天结果表
 insert overwrite table t_mid_biz_auto
select application,
       biz_type,
       new_biz_id,
       net_id,
       pay_id,
       province_id,
       city_id,
       combo_key,
       operate_type,
       cur_product_id,
       new_product_id,
       effect_type,
       sum(case
             when (result = '1') then
              cou
             else
              0
           end),
       sum(case
             when (result <> '1') then
              cou
             else
              0
           end),
      str_to_date(query_date, "yyyyMMdd")
  from t_mid_biz_auto_hour
 where dt >= '${hiveconf:dt}'
   and query_date = '${hiveconf:dt}'
 group by application,
          biz_type,
          new_biz_id,
          net_id,
          pay_id,
          province_id,
          city_id,
          combo_key,
          operate_type,
          cur_product_id,
          new_product_id,
          effect_type,
          str_to_date(query_date, "yyyyMMdd");
