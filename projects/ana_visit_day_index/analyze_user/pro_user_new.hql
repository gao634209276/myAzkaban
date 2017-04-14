CREATE TEMPORARY FUNCTION str_to_date AS 'com.nexr.platform.hive.udf.UDFStrToDate';
CREATE TEMPORARY FUNCTION decode AS 'com.nexr.platform.hive.udf.GenericUDFDecode';
set hive.auto.convert.join=false;
-- 频道/站点 新访客指标  t_ana_nuser,t_ana_nuser_all 
insert overwrite table t_ana_nuser
select total.visit_site,
       total.visit_chid,
      substr(str_to_date('${dayAgo}',  "yyyyMMdd"), 0, 10),
       total.pv,
       total.uv,
       total.ip,
       jumpstay.jumpcnt,
       jumpstay.staycnt,
       jumpstay.sessioncnt
  from (select t1.visit_site, t1.visit_chid,  sum(t1.pv) pv, sum(t1.uv)uv, sum(t1.ip)ip
          from (select visit_site,
                       visit_chid,
                       count(1) pv,
                       count(distinct visit_id) uv,
                       count(distinct visit_ip) ip,
                       visit_id,
                       dt
                  from t_uba_log
                 where dt = '${dayAgo}'
                   and visit_site is not null
                   and visit_chid is not null
                 group by visit_site, visit_chid, visit_id, dt) t1
          join tmp_uba_visit_sid t2 on t1.visit_id = t2.visit_id  and t1.visit_site=t2.visit_site
         where t1.dt = '${dayAgo}'
            and t2.cnt=1 group by t1.visit_site, t1.visit_chid) total
  left join (select visit_site,
                    visit_chid,
                    sum(jump) jumpcnt,
                    round(sum(stay)) staycnt,
                    count(session_id) sessioncnt
               from (select t1.visit_site,
                            t1.visit_chid,
                            session_id,
                            decode(count(1), 1, 1, 0) jump,
                            max(to_unix_timestamp(visit_date)) -
                            min(to_unix_timestamp(visit_date)) stay
                       from t_uba_log t1
                       join tmp_uba_visit_sid t2 on t1.visit_id = t2.visit_id  and t1.visit_site=t2.visit_site
                      where t1.dt = '${dayAgo}' and t2.cnt=1
                        group by t1.visit_site, t1.visit_chid, session_id) tt
              group by tt.visit_site, tt.visit_chid) jumpstay on total.visit_site =
                                                                 jumpstay.visit_site
                                                             and total.visit_chid =
                                                                 jumpstay.visit_chid;

-----------------------------------------------------------------------------------------------
-- 频道/站点 新访客入口页面PV t_ana_nuser_in ,t_ana_nuser_in_all
insert overwrite table t_ana_nuser_in
select t3.visit_site,
       t3.visit_chid,
       t3.refer_url,
      substr(str_to_date('${dayAgo}',  "yyyyMMdd"), 0, 10),
         count(t3.visit_url) pv
  from (select visit_site, visit_chid, session_id, min((visit_date)) mindate, dt
          from t_uba_log
         where visit_site is not null
           and visit_chid is not null
           and session_id is not null
           and visit_url is not null
         group by visit_site, visit_chid, session_id, dt) t1
  left join t_uba_log t2 on t1.visit_site = t2.visit_site
                        and t1.visit_chid = t2.visit_chid
                        and t1.session_id = t2.session_id
                        and t1.mindate = (t2.visit_date)
  left join t_uba_log t3 on t3.visit_site = t2.visit_site
                        and t3.visit_chid = t2.visit_chid
                        and t3.session_id = t2.session_id
                        and t3.refer_url = t2.visit_url
  join tmp_uba_visit_sid t4 on t3.visit_id = t4.visit_id  and t3.visit_site=t4.visit_site
 where t3.refer_url is not null
 and t4.cnt=1 
   and t1.dt = '${dayAgo}'
   and t2.dt = '${dayAgo}'
   and t3.dt = '${dayAgo}'
   group by t3.visit_site, t3.visit_chid, t3.refer_url;
   
-----------------------------------------------------------------------------------------------                                                 
 -- 频道/站点 新访客来源UV  t_ana_nuser_source  t_ana_nuser_source_all
insert overwrite table t_ana_nuser_source
 select t1.visit_site,
        t1.visit_chid,
        channel,
         substr(str_to_date('${dayAgo}',  "yyyyMMdd"), 0, 10),
         count(distinct t1.visit_id) uv
   from (select visit_site, visit_chid, channel, visit_id
           from t_uba_log
          where dt = '${dayAgo}'
            and visit_site is not null
            and visit_chid is not null
            and channel is not null) t1
   join tmp_uba_visit_sid t2 on t1.visit_id = t2.visit_id   and t1.visit_site=t2.visit_site
   where t2.cnt=1
  group by t1.visit_site, t1.visit_chid, channel;