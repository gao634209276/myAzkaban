CREATE TEMPORARY FUNCTION str_to_date AS 'com.nexr.platform.hive.udf.UDFStrToDate';
CREATE TEMPORARY FUNCTION decode AS 'com.nexr.platform.hive.udf.GenericUDFDecode';
set hive.auto.convert.join=false;
-- 来源统计指标
--频道每种来源统计指标  表：tt_ana_sources_detal      字段： visit_site, visit_chid , channel
--频道全部来源统计指标  表：t_ana_sources             字段： visit_site, visit_chid
--站点每种来源统计指标  表：tt_ana_sources_detal_all  字段： visit_site,              channel
insert overwrite table t_ana_sources_detal
  select   total.visit_chid,
         total.channel,
          substr(str_to_date('${dayAgo}',  "yyyyMMdd"), 0, 10),
         total.pv,
         total.uv,
         total.ip,
         jumpstay.jumpcnt,
         jumpstay.staycnt,
         total.visit_site,
         jumpstay.sessioncnt
    from (select visit_site,
                 visit_chid,
                 channel,
                  count(1) pv,
                 count(distinct visit_id) uv,
                 count(distinct visit_ip) ip
            from t_uba_log
           where dt = '${dayAgo}'
             and visit_site is not null
             and visit_chid is not null
             and channel is not null
           group by visit_site, visit_chid, channel) total
    left join (select sum(jump) jumpcnt,
                      round(sum(stay)) staycnt,
                      count(session_id) sessioncnt,
                      visit_site,
                      visit_chid,
                      channel
                 from (select visit_site,
                              visit_chid,
                              session_id,
                              channel,
                               decode(count(1), 1, 1, 0) jump,
                              max(to_unix_timestamp(visit_date)) -
                              min(to_unix_timestamp(visit_date)) stay
                         from t_uba_log
                        where dt = '${dayAgo}'
                          and visit_site is not null
                          and visit_chid is not null
                          and channel is not null
                          and session_id is not null
                        group by visit_site, visit_chid, channel, session_id) tt
                group by tt.visit_site, tt.visit_chid, tt.channel) jumpstay on total.visit_site =
                                                                               jumpstay.visit_site
                                                                           and total.visit_chid =
                                                                               jumpstay.visit_chid
                                                                           and total.channel =
                                                                               jumpstay.channel;
--频道全部来源统计指标  表：t_ana_sources             字段： visit_site, visit_chid
insert overwrite table t_ana_sources
  select total.visit_site,
         total.visit_chid,
           substr(str_to_date('${dayAgo}',  "yyyyMMdd"), 0, 10),
          total.pv,
         total.uv,
         total.ip,
         jumpstay.jumpcnt,
         jumpstay.staycnt,
         jumpstay.sessioncnt
    from (select visit_site,
                 visit_chid,
                 count(1) pv,
                 count(distinct visit_id) uv,
                 count(distinct visit_ip) ip
            from t_uba_log
           where dt = '${dayAgo}'
             and visit_site is not null
             and visit_chid is not null
            group by visit_site, visit_chid) total
    left join (select sum(jump) jumpcnt,
                      sum(stay)  staycnt,
                      count(session_id) sessioncnt,
                      visit_site,
                      visit_chid
                 from (select visit_site,
                              visit_chid,
                              session_id,
                               decode(count(1), 1, 1, 0) jump,
                              max(to_unix_timestamp(visit_date)) -
                              min(to_unix_timestamp(visit_date)) stay
                         from t_uba_log
                        where dt = '${dayAgo}'
                          and visit_site is not null
                          and visit_chid is not null
                          and session_id is not null
                        group by visit_site, visit_chid, session_id) tt
                group by tt.visit_site, tt.visit_chid) jumpstay on total.visit_site =
                                                                               jumpstay.visit_site
                                                                           and total.visit_chid =
                                                                               jumpstay.visit_chid;
 --站点每种来源统计指标  表：tt_ana_sources_detal_all  字段： visit_site, channel
insert overwrite table t_ana_sources_detal_all
  select total.visit_site,
         total.channel,
          substr(str_to_date('${dayAgo}',  "yyyyMMdd"), 0, 10),
         total.pv,
         total.uv,
         total.ip,
         jumpstay.jumpcnt,
         jumpstay.staycnt,
         jumpstay.sessioncnt
    from (select visit_site,
                 channel,
                   count(1) pv,
                 count(distinct visit_id) uv,
                 count(distinct visit_ip) ip
            from t_uba_log
           where dt = '${dayAgo}'
             and visit_site is not null
              and channel is not null
           group by visit_site, channel) total
    left join (select sum(jump) jumpcnt,
                      sum(stay) staycnt,
                      count(session_id) sessioncnt,
                      visit_site,
                        channel
                 from (select visit_site,
                               session_id,
                              channel,
                               decode(count(1), 1, 1,0) jump,
                              max(to_unix_timestamp(visit_date)) -
                              min(to_unix_timestamp(visit_date)) stay
                         from t_uba_log
                        where dt = '${dayAgo}'
                          and visit_site is not null
                           and channel is not null
                          and session_id is not null
                        group by visit_site, channel, session_id) tt
                group by tt.visit_site, tt.channel) jumpstay on total.visit_site =
                                                                               jumpstay.visit_site
                                                                           and total.channel =
                                                                               jumpstay.channel;