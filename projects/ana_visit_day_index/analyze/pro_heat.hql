CREATE TEMPORARY FUNCTION str_to_date AS 'com.nexr.platform.hive.udf.UDFStrToDate';
set hive.auto.convert.join=false;
--热力图
insert overwrite table t_ana_heat 
select visit_url,
       concat_ws(',', collect_set(optionts)),
         substr(str_to_date('${dayAgo}',  "yyyyMMdd"), 0, 10),
        max(cou)
  from (select visit_url,
  			    cou,
               concat('{x:', x, ',y:', y, ',value:', cou, '}') optionts
          from (select visit_url,
          			    substr(data_points, 0, instr(data_points, '-') - 1) x,
                       substr(data_points,
                              instr(data_points, '-') + 1,
                              length(data_points)) y,
                       data_points,
                       count(1) cou
                  from t_uba_heat_log
                 where dt = '${dayAgo}'
                 group by visit_url, data_points) t 
                 where x > 0  and y > 0) tt
 group by visit_url;
 