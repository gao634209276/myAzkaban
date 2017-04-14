CREATE TEMPORARY FUNCTION str_to_date AS 'com.nexr.platform.hive.udf.UDFStrToDate';
CREATE TEMPORARY FUNCTION decode AS 'com.nexr.platform.hive.udf.GenericUDFDecode';
set hive.auto.convert.join=false;
-- 频道/站点 新访客指标  t_ana_nuser,t_ana_nuser_all 
insert overwrite table t_ana_nuser_all
select total.visit_site,
       substr(str_to_date('${dayAgo}', "yyyyMMdd"), 0, 10),
       total.pv,
       total.uv,
       total.ip,
       jumpstay.jumpcnt,
       jumpstay.staycnt,
       jumpstay.sessioncnt
  from (select visit_site,
               count(1) pv,
               count(distinct visit_id) uv,
               count(distinct visit_ip) ip
          from t_uba_log t
         where dt = '${dayAgo}'
           and visit_site is not null
           and t.visit_id in
               (select visit_id from tmp_uba_visit_sid where cnt = 1)
         group by visit_site) total
  left join (select visit_site,
                    sum(jump) jumpcnt,
                    round(sum(stay)) staycnt,
                    count(session_id) sessioncnt
               from (select visit_site,
                            session_id,
                            decode(count(1), 1, 1, 0) jump,
                            max(to_unix_timestamp(visit_date)) -
                            min(to_unix_timestamp(visit_date)) stay
                       from t_uba_log t
                      where dt = '${dayAgo}'
                        and visit_site is not null
                        and t.visit_id in (select visit_id
                                             from tmp_uba_visit_sid
                                            where cnt = 1)
                      group by visit_site, session_id) tt
              group by tt.visit_site) jumpstay on total.visit_site =
                                                  jumpstay.visit_site;

-----------------------------------------------------------------------------------------------
-- 频道/站点 新访客入口页面PV t_ana_nuser_in ,t_ana_nuser_in_all
insert overwrite table t_ana_nuser_in_all
select t3.visit_site,
       t3.refer_url,
       substr(str_to_date('${dayAgo}',  "yyyyMMdd"), 0, 10),
         count(t3.visit_url) pv
  from (select visit_site, session_id, min(to_unix_timestamp(visit_date)) mindate, dt
          from t_uba_log
         where visit_site is not null
             and session_id is not null
           and visit_url is not null
         group by visit_site, session_id, dt) t1
  left join t_uba_log t2 on t1.visit_site = t2.visit_site
                           and t1.session_id = t2.session_id
                        and t1.mindate = to_unix_timestamp(t2.visit_date)
  left join t_uba_log t3 on t3.visit_site = t2.visit_site
                         and t3.session_id = t2.session_id
                        and t3.refer_url = t2.visit_url
  join tmp_uba_visit_sid t4 on t3.visit_id = t4.visit_id and t3.visit_site=t4.visit_site
 where t3.refer_url is not null
 and t4.cnt='1'
   and t1.dt = '${dayAgo}'
   and t2.dt = '${dayAgo}'
   and t3.dt = '${dayAgo}'
   group by t3.visit_site, t3.refer_url;
   
-----------------------------------------------------------------------------------------------                                                 
 -- 频道/站点 新访客来源UV  t_ana_nuser_source  t_ana_nuser_source_all
insert overwrite table t_ana_nuser_source_all
 select t1.visit_site,
        channel,
         substr(str_to_date('${dayAgo}',  "yyyyMMdd"), 0, 10),
         count(distinct t1.visit_id) uv
   from (select visit_site, channel, visit_id
           from t_uba_log
          where dt = '${dayAgo}'
            and visit_site is not null
             and channel is not null) t1
   join tmp_uba_visit_sid t2 on t1.visit_id = t2.visit_id  and t1.visit_site=t2.visit_site
   where t2.cnt='1'
  group by t1.visit_site, channel;