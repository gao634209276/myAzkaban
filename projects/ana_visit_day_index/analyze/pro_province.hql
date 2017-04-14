CREATE TEMPORARY FUNCTION str_to_date AS 'com.nexr.platform.hive.udf.UDFStrToDate';
CREATE TEMPORARY FUNCTION decode AS 'com.nexr.platform.hive.udf.GenericUDFDecode';
set hive.auto.convert.join=false;
-- 频道/站点 每个省份指标 t_ana_province t_ana_province_all
insert overwrite table t_ana_province
 select total.visit_site,
        total.visit_chid,
        total.visit_pro,
           substr(str_to_date('${dayAgo}',  "yyyyMMdd"), 0, 10),
         total.pv,
        total.uv,
        total.ip,
        jumpstay.jumpcnt,
        jumpstay.staycnt,
        jumpstay.sessioncnt
   from (select visit_site,
                visit_chid,
                visit_pro,
                 count(1) pv,
                count(distinct visit_id) uv,
                count(distinct visit_ip) ip
           from t_uba_log
          where dt = '${dayAgo}'
            and visit_site is not null
            and visit_chid is not null
            and visit_pro is not null
          group by visit_site, visit_chid, visit_pro) total
   left join (select sum(jump) jumpcnt,
                     round(sum(stay)) staycnt,
                     count(session_id) sessioncnt,
                     visit_site,
                     visit_chid,
                     visit_pro
                from (select visit_site,
                             visit_chid,
                             visit_pro,
                             session_id,
                             decode(count(visit_site), 1, 1, 0) jump,
                             max(to_unix_timestamp(visit_date)) -
                             min(to_unix_timestamp(visit_date)) stay
                        from t_uba_log
                       where dt = '${dayAgo}'
                         and visit_site is not null
                         and visit_chid is not null
                         and visit_pro is not null
                         and session_id is not null
                       group by visit_site, visit_chid, visit_pro, session_id) tt
               group by tt.visit_site, tt.visit_chid, tt.visit_pro) jumpstay on total.visit_site =
                                                                                jumpstay.visit_site
                                                                            and total.visit_chid =
                                                                                jumpstay.visit_chid
                                                                            and total.visit_pro =
                                                                                jumpstay.visit_pro;
--站点
 insert overwrite table t_ana_province_all
 select total.visit_site,
        total.visit_pro,
       substr(str_to_date('${dayAgo}',  "yyyyMMdd"), 0, 10),
         total.pv,
        total.uv,
        total.ip,
        jumpstay.jumpcnt,
        jumpstay.staycnt,
        jumpstay.sessioncnt
   from (select visit_site,
                visit_pro,
                  count(1) pv,
                count(distinct visit_id) uv,
                count(distinct visit_ip) ip
           from t_uba_log
          where dt = '${dayAgo}'
            and visit_site is not null
             and visit_pro is not null
          group by visit_site, visit_pro) total
   left join (select sum(jump) jumpcnt,
                     round(sum(stay)) staycnt,
                     count(session_id) sessioncnt,
                     visit_site,
                      visit_pro
                from (select visit_site,
                              visit_pro,
                             session_id,
                             decode(count(visit_site), 1, 1, 0) jump,
                             max(to_unix_timestamp(visit_date)) -
                             min(to_unix_timestamp(visit_date)) stay
                        from t_uba_log
                       where dt = '${dayAgo}'
                         and visit_site is not null
                           and visit_pro is not null
                         and session_id is not null
                       group by visit_site, visit_pro, session_id) tt
               group by tt.visit_site, tt.visit_pro) jumpstay on total.visit_site =
                                                                                jumpstay.visit_site
                                                                              and total.visit_pro =
                                                                                jumpstay.visit_pro;