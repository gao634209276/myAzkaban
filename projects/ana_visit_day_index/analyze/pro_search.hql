CREATE TEMPORARY FUNCTION str_to_date AS 'com.nexr.platform.hive.udf.UDFStrToDate';
CREATE TEMPORARY FUNCTION decode AS 'com.nexr.platform.hive.udf.GenericUDFDecode';
set hive.auto.convert.join=false;
-- 频道/站点 每种搜索引擎统计指标 t_ana_search t_ana_search_all
insert overwrite table t_ana_search
  select total.visit_site,
         total.visit_chid,
         total.search,
          substr(str_to_date('${dayAgo}',  "yyyyMMdd"), 0, 10),
           total.pv,
         total.uv,
         total.ip,
         jumpstay.jumpcnt,
         jumpstay.staycnt,
         jumpstay.sessioncnt
    from (select visit_site,
                 visit_chid,
                 search,
                  count(1) pv,
                 count(distinct visit_id) uv,
                 count(distinct visit_ip) ip
            from t_uba_log
           where dt = '${dayAgo}'
             and channel = '1'
             and visit_site is not null
             and visit_chid is not null
             and search is not null
           group by visit_site, visit_chid, search) total
    left join (select sum(jump) jumpcnt,
                      round(sum(stay)) staycnt,
                      count(session_id) sessioncnt,
                      visit_site,
                      visit_chid,
                      search
                 from (select visit_site,
                              visit_chid,
                              session_id,
                              search,
                              decode(count(visit_site), 1, 1, 0) jump,
                              max(to_unix_timestamp(visit_date)) -
                              min(to_unix_timestamp(visit_date)) stay
                         from t_uba_log
                        where dt = '${dayAgo}'
                          and channel = '1'
                          and visit_site is not null
                          and visit_chid is not null
                          and search is not null
                          and session_id is not null
                        group by visit_site, visit_chid, search, session_id) tt
                group by tt.visit_site, tt.visit_chid, tt.search) jumpstay on total.visit_site =
                                                                              jumpstay.visit_site
                                                                          and total.visit_chid =
                                                                              jumpstay.visit_chid
                                                                          and total.search =
                                                                              jumpstay.search;
--  站点 每种搜索引擎统计指标   t_ana_search_all
insert overwrite table t_ana_search_all
  select total.visit_site,
         total.search,
          substr(str_to_date('${dayAgo}',  "yyyyMMdd"), 0, 10),
          total.pv,
         total.uv,
         total.ip,
         jumpstay.jumpcnt,
         jumpstay.staycnt,
         jumpstay.sessioncnt
    from (select visit_site,
                 search,
                 count(1) pv,
                 count(distinct visit_id) uv,
                 count(distinct visit_ip) ip
            from t_uba_log
           where dt = '${dayAgo}'
             and channel = '1'
             and visit_site is not null
               and search is not null
           group by visit_site, search) total
    left join (select sum(jump) jumpcnt,
                      round(sum(stay)) staycnt,
                      count(session_id) sessioncnt,
                      visit_site,
                        search
                 from (select visit_site,
                               session_id,
                              search,
                              decode(count(visit_site), 1, 1, 0) jump,
                              max(to_unix_timestamp(visit_date)) -
                              min(to_unix_timestamp(visit_date)) stay
                         from t_uba_log
                        where dt = '${dayAgo}'
                          and channel = '1'
                          and visit_site is not null
                            and search is not null
                          and session_id is not null
                        group by visit_site, search, session_id) tt
                group by tt.visit_site, tt.search) jumpstay on total.visit_site =
                                                                              jumpstay.visit_site
                                                                             and total.search =
                                                                              jumpstay.search;