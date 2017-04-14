CREATE TEMPORARY FUNCTION str_to_date AS 'com.nexr.platform.hive.udf.UDFStrToDate';
CREATE TEMPORARY FUNCTION decode AS 'com.nexr.platform.hive.udf.GenericUDFDecode';
set hive.auto.convert.join=false;
--网页下游PV,网页上游PV,跳出率统计                                                   
-----------------------------------------------------------------------------------------------
-- 网页下游PV
insert overwrite table t_ana_down
select   substr(str_to_date('${dayAgo}',  "yyyyMMdd"), 0, 10),
	    t1.refer_url,
       t1.visit_url,
       t1.urlcount
  from (select refer_url, visit_url, count(1) urlcount
          from t_uba_log tul
         where dt = '${dayAgo}'
           and visit_url is not null
             and  refer_url is not null
             and visit_site is not null
         group by refer_url, visit_url) t1
  join t_config_up_down_stream t2 on t1.refer_url = t2.strm_urladdr;

-----------------------------------------------------------------------------------------------
-- 网页上游PV
insert overwrite table t_ana_up
select   substr(str_to_date('${dayAgo}',  "yyyyMMdd"), 0, 10),
        t1.refer_url,
       t1.visit_url,
       t1.urlcount
  from (select refer_url, visit_url, count(1) urlcount
          from t_uba_log tul
         where dt = '${dayAgo}'
           and visit_url is not null
             and  refer_url is not null
              and visit_site is not null
         group by refer_url, visit_url) t1
  join t_config_up_down_stream t2 on t1.visit_url = t2.strm_urladdr;
  
  
  --跳出率统计 
insert overwrite table t_ana_down_jump
  select   substr(str_to_date('${dayAgo}',  "yyyyMMdd"), 0, 10),
  		  t3.strm_urladdr,
         (nvl(total, 0) - nvl(refer_count, 0)) as jump
    from (select t2.strm_urladdr, count(1) total
            from t_uba_log t1
            join t_config_up_down_stream t2 on t1.visit_url =
                                               t2.strm_urladdr
           where dt = '${dayAgo}'
             and t1.visit_url is not null
             and t1.refer_url is not null
              and t1.visit_site is not null
           group by t2.strm_urladdr) t3
    full join (select t2.strm_urladdr, count(1) refer_count
                 from t_uba_log t1
                 join t_config_up_down_stream t2 on t1.refer_url =
                                                    t2.strm_urladdr
                where dt = '${dayAgo}'
                  and t1.visit_url is not null
             and t1.refer_url is not null
              and t1.visit_site is not null
                group by t2.strm_urladdr) t4 on t3.strm_urladdr =
                                                t4.strm_urladdr
   where nvl(t3.total,0) - nvl(t4.refer_count,0) > 0;
  


