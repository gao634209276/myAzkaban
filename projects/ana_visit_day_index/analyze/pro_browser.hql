CREATE TEMPORARY FUNCTION str_to_date AS 'com.nexr.platform.hive.udf.UDFStrToDate';
CREATE TEMPORARY FUNCTION decode AS 'com.nexr.platform.hive.udf.GenericUDFDecode';
set hive.auto.convert.join=false;
-- 频道/站点   每种浏览器指标
--频道 t_ana_browser 
--站点 t_ana_browser_all 
insert overwrite table t_ana_browser
  select total.visit_site,
         total.visit_chid,
         total.browser,
          substr(str_to_date('${dayAgo}',  "yyyyMMdd"), 0, 10),
           total.pv,
         total.uv,
         total.ip,
         jumpstay.jumpcnt,
         jumpstay.staycnt,
         jumpstay.sessioncnt
    from (select visit_site,
                 visit_chid,
                 browser,
                  count(1) pv,
                 count(distinct visit_id) uv,
                 count(distinct visit_ip) ip
            from t_uba_log tultotal
           where dt = '${dayAgo}'
             and visit_site is not null
             and visit_chid is not null
             and browser is not null
           group by visit_site, visit_chid, browser) total
    left join (select sum(ins.jump) jumpcnt,
                      sum(ins.stay) staycnt,
                      count(ins.session_id) sessioncnt,
                      ins.visit_site,
                      ins.visit_chid,
                      ins.browser
                 from (select visit_site,
                              visit_chid,
                              browser,
                              session_id,
                              decode(count(visit_site), 1, 1, 0) jump,
                              max(to_unix_timestamp(visit_date)) -
                              min(to_unix_timestamp(visit_date)) stay
                         from t_uba_log tulj
                        where dt = '${dayAgo}'
                          and visit_site is not null
                          and visit_chid is not null
                          and browser is not null
                          and session_id is not null
                        group by visit_site, visit_chid, browser, session_id) ins
                group by ins.visit_site, ins.visit_chid, ins.browser) jumpstay on total.visit_site =
                                                                                  jumpstay.visit_site
                                                                              and total.visit_chid =
                                                                                  jumpstay.visit_chid
                                                                              and total.browser =
                                                                                  jumpstay.browser;
--站点 t_ana_browser_all 
insert overwrite table t_ana_browser_all
  select total.visit_site,
         total.browser,
        substr(str_to_date('${dayAgo}',  "yyyyMMdd"), 0, 10),
           total.pv,
         total.uv,
         total.ip,
         jumpstay.jumpcnt,
         jumpstay.staycnt,
         jumpstay.sessioncnt
    from (select visit_site,
                 browser,
                  count(1) pv,
                 count(distinct visit_id) uv,
                 count(distinct visit_ip) ip
            from t_uba_log tultotal
           where dt = '${dayAgo}'
             and visit_site is not null
               and browser is not null
           group by visit_site, browser) total
    left join (select sum(ins.jump) jumpcnt,
                     sum(ins.stay) staycnt,
                      count(ins.session_id) sessioncnt,
                      ins.visit_site,
                        ins.browser
                 from (select visit_site,
                               browser,
                              session_id,
                              decode(count(visit_site), 1, 1, 0) jump,
                              max(to_unix_timestamp(visit_date)) -
                              min(to_unix_timestamp(visit_date)) stay
                         from t_uba_log tulj
                        where dt = '${dayAgo}'
                          and visit_site is not null
                           and browser is not null
                          and session_id is not null
                        group by visit_site, browser, session_id) ins
                group by ins.visit_site, ins.browser) jumpstay on total.visit_site =
                                                                                  jumpstay.visit_site
                                                                               and total.browser =
                                                                                  jumpstay.browser;

  