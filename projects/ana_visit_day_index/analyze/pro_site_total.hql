CREATE TEMPORARY FUNCTION str_to_date AS 'com.nexr.platform.hive.udf.UDFStrToDate';
CREATE TEMPORARY FUNCTION decode AS 'com.nexr.platform.hive.udf.GenericUDFDecode';
set hive.auto.convert.join=false;
---  站点被访页面PV ,站点来源域名数量,站点的统计指标
-- 站点被访页面PV
insert overwrite table t_ana_accessed
  select visit_site,
         substr(str_to_date('${dayAgo}',  "yyyyMMdd"), 0, 10),
           visit_url,
         count(1) urlcount
    from t_uba_log
   where dt = '${dayAgo}'
     and visit_site is not null
     and visit_domain is not null
   group by visit_site, visit_url;
 -----------------------------------------------------------------------------------------------
-- 站点来源域名数量
insert overwrite table t_ana_refer
  select visit_site,
          substr(str_to_date('${dayAgo}',  "yyyyMMdd"), 0, 10),
          refer_domain,
         count(1) refcount
    from t_uba_log
   where dt = '${dayAgo}'
     and visit_site is not null
     and refer_domain is not null
   group by visit_site, refer_domain;
  
 -----------------------------------------------------------------------------------------------
-- 站点的统计指标
insert overwrite table t_ana_system 
 select total.visit_site,
       substr(str_to_date('${dayAgo}',  "yyyyMMdd"), 0, 10),
        total.pv,
        total.uv,
        total.ip,
        jumpstay.jumpcnt,
        jumpstay.staycnt,
        jumpstay.sessioncnt
   from (select visit_site,
                count(visit_url) pv,
                count(distinct visit_id) uv,
                count(distinct visit_ip) ip
           from t_uba_log tbl
          where dt = '${dayAgo}'
            and visit_site is not null
          group by visit_site) total
   left join (select sum(t.jump) jumpcnt,
                     round(sum(t.stay)) staycnt,
                     count(t.session_id) sessioncnt,
                     t.visit_site
                from (select visit_site,
                             session_id,
                             decode(count(visit_site), 1, 1, 0) jump,
                             max(to_unix_timestamp(visit_date)) -
                             min(to_unix_timestamp(visit_date)) stay
                        from t_uba_log
                       where dt = '${dayAgo}'
                         and visit_site is not null
                       group by visit_site, session_id) t
               group by t.visit_site) jumpstay on total.visit_site =
                                                  jumpstay.visit_site;