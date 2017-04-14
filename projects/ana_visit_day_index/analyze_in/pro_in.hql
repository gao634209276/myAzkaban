CREATE TEMPORARY FUNCTION str_to_date AS 'com.nexr.platform.hive.udf.UDFStrToDate';
CREATE TEMPORARY FUNCTION decode AS 'com.nexr.platform.hive.udf.GenericUDFDecode';
set hive.auto.convert.join=false;
-- 频道/站点 入口页面指标  t_ana_in  t_ana_in_all
insert overwrite table t_ana_in
  select total.visit_site,
         total.visit_chid,
         substr(str_to_date('${dayAgo}', "yyyyMMdd"), 0, 10),
         total.pv,
         total.uv,
         total.ip,
         jumpstay.jump,
         jumpstay.stay,
         jumpstay.sessioncnt
    from (select t3.visit_site,
                 t3.visit_chid,
                 count(t3.visit_url) pv,
                 count(distinct t3.visit_id) uv,
                 count(distinct t3.visit_ip) ip
            from (select visit_site,
                         visit_chid,
                         session_id,
                         min(visit_date) mindate
                    from t_uba_log
                   where dt = '${dayAgo}'
                     and visit_site is not null
                     and visit_chid is not null
                     and session_id is not null
                     and visit_url is not null
                   group by visit_site, visit_chid, session_id) t1
            left join (select visit_site,
                             visit_chid,
                             session_id,
                             visit_date,
                             visit_url
                        from t_uba_log
                       where dt = '${dayAgo}') t2 on t1.visit_site =
                                                    t2.visit_site
                                                and t1.visit_chid =
                                                    t2.visit_chid
                                                and t1.session_id =
                                                    t2.session_id
                                                and t1.mindate =
                                                    t2.visit_date
              join (select visit_site,
                             visit_chid,
                             session_id,
                             refer_url,
                             visit_url,
                             visit_id,
                             visit_ip
                        from t_uba_log
                       where dt = '${dayAgo}') t3 on t3.visit_site =
                                                    t2.visit_site
                                                and t3.visit_chid =
                                                    t2.visit_chid
                                                and t3.session_id =
                                                    t2.session_id
                                                and t3.refer_url =
                                                    t2.visit_url
           group by t3.visit_site, t3.visit_chid) total
    left join (select visit_site,
                      visit_chid,
                      sum(jump) jump,
                      sum(nvl(stay, 0)) stay,
                      count(session_id) sessioncnt
                 from (select t3.visit_site,
                              t3.visit_chid,
                              t3.session_id,
                              decode(count(t3.visit_url), 1, 1, 0) jump,
                               min(to_unix_timestamp(t3.visit_date)) -
                              min(to_unix_timestamp(t1.mindate))stay
                         from (select visit_site,
                                      visit_chid,
                                      session_id,
                                      min(visit_date) mindate
                                 from t_uba_log
                                where dt = '${dayAgo}'
                                  and visit_site is not null
                                  and visit_chid is not null
                                  and session_id is not null
                                  and visit_url is not null
                                group by visit_site, visit_chid, session_id) t1
                         left join (select visit_site,
                                          visit_chid,
                                          session_id,
                                          visit_date,
                                          visit_url
                                     from t_uba_log
                                    where dt = '${dayAgo}'
                                      and visit_site is not null
                                      and visit_chid is not null) t2 on t1.visit_site =
                                                                        t2.visit_site
                                                                    and t1.visit_chid =
                                                                        t2.visit_chid
                                                                    and t1.session_id =
                                                                        t2.session_id
                                                                    and t1.mindate =
                                                                        t2.visit_date
                           join (select visit_site,
                                          visit_chid,
                                          session_id,
                                          refer_url,
                                          visit_url,
                                          visit_date
                                     from t_uba_log
                                    where dt = '${dayAgo}'
                                      and visit_site is not null
                                      and visit_chid is not null
                                      and visit_url is not null) t3 on t3.visit_site =
                                                                       t2.visit_site
                                                                   and t3.visit_chid =
                                                                       t2.visit_chid
                                                                   and t3.session_id =
                                                                       t2.session_id
                                                                   and t3.refer_url =
                                                                       t2.visit_url
                        group by t3.visit_site, t3.visit_chid, t3.session_id)t
                group by visit_site, visit_chid) jumpstay on total.visit_site =
                                                             jumpstay.visit_site
                                                         and total.visit_chid =
                                                             jumpstay.visit_chid;

--  站点 入口页面指标     t_ana_in_all
insert overwrite table t_ana_in_all
  select total.visit_site,
         substr(str_to_date('${dayAgo}', "yyyyMMdd"), 0, 10),
         total.pv,
         total.uv,
         total.ip,
         jumpstay.jump,
         jumpstay.stay,
         jumpstay.sessioncnt
    from (select t3.visit_site,
                 count(t3.visit_url) pv,
                 count(distinct t3.visit_id) uv,
                 count(distinct t3.visit_ip) ip
            from (select visit_site, session_id, min(visit_date) mindate
                    from t_uba_log
                   where dt = '${dayAgo}'
                     and visit_site is not null
                     and session_id is not null
                     and visit_url is not null
                   group by visit_site, session_id) t1
            left join (select visit_site, session_id, visit_date, visit_url
                        from t_uba_log
                       where dt = '${dayAgo}') t2 on t1.visit_site =
                                                    t2.visit_site
                                                and t1.session_id =
                                                    t2.session_id
                                                and t1.mindate =
                                                    t2.visit_date
            join (select visit_site,
                        session_id,
                        refer_url,
                        visit_url,
                        visit_id,
                        visit_ip
                   from t_uba_log
                  where dt = '${dayAgo}') t3 on t3.visit_site = t2.visit_site
                                           and t3.session_id = t2.session_id
                                           and t3.refer_url = t2.visit_url
           group by t3.visit_site) total
    left join (select visit_site,
                      sum(jump) jump,
                      sum(nvl(stay, 0)) stay,
                      count(session_id) sessioncnt
                 from (select t3.visit_site,
                              t3.session_id,
                              decode(count(t3.visit_url), 1, 1, 0) jump,
                             min(to_unix_timestamp(t3.visit_date)) -
                              min(to_unix_timestamp(t1.mindate))stay
                         from (select visit_site,
                                      session_id,
                                      min(visit_date) mindate
                                 from t_uba_log
                                where dt = '${dayAgo}'
                                  and visit_site is not null
                                  and session_id is not null
                                  and visit_url is not null
                                group by visit_site, session_id) t1
                         left join (select visit_site,
                                          session_id,
                                          visit_date,
                                          visit_url
                                     from t_uba_log
                                    where dt = '${dayAgo}'
                                      and visit_site is not null) t2 on t1.visit_site =
                                                                        t2.visit_site
                                                                    and t1.session_id =
                                                                        t2.session_id
                                                                    and t1.mindate =
                                                                        t2.visit_date
                         join (select visit_site,
                                     session_id,
                                     refer_url,
                                     visit_url,
                                     visit_date
                                from t_uba_log
                               where dt = '${dayAgo}'
                                 and visit_site is not null
                                 and visit_url is not null) t3 on t3.visit_site =
                                                                  t2.visit_site
                                                              and t3.session_id =
                                                                  t2.session_id
                                                              and t3.refer_url =
                                                                  t2.visit_url
                        group by t3.visit_site, t3.session_id) t
                group by visit_site) jumpstay on total.visit_site =
                                                 jumpstay.visit_site;
