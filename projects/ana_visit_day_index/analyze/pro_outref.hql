CREATE TEMPORARY FUNCTION str_to_date AS 'com.nexr.platform.hive.udf.UDFStrToDate';
CREATE TEMPORARY FUNCTION decode AS 'com.nexr.platform.hive.udf.GenericUDFDecode';
set hive.auto.convert.join=false;
-- 频道/站点 外部链接（域名）统计指标 t_ana_outref,t_ana_outref_all
insert overwrite table t_ana_outref
  select total.visit_site,
         total.visit_chid,
         total.refer_domain,
         substr(str_to_date('${dayAgo}', "yyyyMMdd"), 0, 10),
         total.pv,
         total.uv,
         total.ip,
         jumpstay.jumpcnt,
         jumpstay.staycnt,
         jumpstay.sessioncnt
    from (select visit_site,
                 visit_chid,
                 refer_domain,
                 count(visit_url) pv,
                 count(distinct visit_id) uv,
                 count(distinct visit_ip) ip
            from t_uba_log
           where dt = '${dayAgo}'
             and channel = '3'
             and visit_site is not null
             and visit_chid is not null
             and refer_domain is not null
           group by visit_site, visit_chid, refer_domain) total
    left join (select sum(jump) jumpcnt,
                      sum(stay) staycnt,
                      count(session_id) sessioncnt,
                      visit_site,
                      visit_chid,
                      refer_domain
                 from (select visit_site,
                              visit_chid,
                              session_id,
                              refer_domain,
                              decode(count(visit_site), 1, 1, 0) jump,
                              max(to_unix_timestamp(visit_date)) - min(to_unix_timestamp(visit_date)) stay
                         from t_uba_log
                        where dt = '${dayAgo}'
                          and channel = '3'
                          and visit_site is not null
                          and visit_chid is not null
                          and refer_domain is not null
                          and session_id is not null
                        group by visit_site,
                                 visit_chid,
                                 refer_domain,
                                 session_id)t
                group by visit_site, visit_chid, refer_domain) jumpstay on total.visit_site =
                                                                           jumpstay.visit_site
                                                                       and total.visit_chid =
                                                                           jumpstay.visit_chid
                                                                       and total.refer_domain =
                                                                           jumpstay.refer_domain;
  --  站点 外部链接（域名）统计指标   t_ana_outref_all

insert overwrite table  t_ana_outref_all
      select total.visit_site,
           total.refer_domain,
           substr(str_to_date('${dayAgo}', "yyyyMMdd"), 0, 10),
           total.pv,
           total.uv,
           total.ip,
           jumpstay.jumpcnt,
           jumpstay.staycnt,
           jumpstay.sessioncnt
      from (select visit_site,
                   refer_domain,
                   count(visit_url) pv,
                   count(distinct visit_id) uv,
                   count(distinct visit_ip) ip
              from t_uba_log
             where dt = '${dayAgo}'
               and channel = '3'
               and visit_site is not null
               and refer_domain is not null
             group by visit_site, refer_domain) total
      left join (select sum(jump) jumpcnt,
                         sum(stay)  staycnt,
                        count(session_id) sessioncnt,
                        visit_site,
                        refer_domain
                   from (select visit_site,
                                session_id,
                                refer_domain,
                               decode(count(visit_site), 1, 1, 0) jump,
                              max(to_unix_timestamp(visit_date)) - min(to_unix_timestamp(visit_date)) stay
                           from t_uba_log
                          where  dt = '${dayAgo}'
                            and channel = '3'
                            and visit_site is not null
                            and refer_domain is not null
                            and session_id is not null
                          group by visit_site, refer_domain, session_id)t
                  group by visit_site, refer_domain) jumpstay
        on total.visit_site = jumpstay.visit_site
       and total.refer_domain = jumpstay.refer_domain;
 