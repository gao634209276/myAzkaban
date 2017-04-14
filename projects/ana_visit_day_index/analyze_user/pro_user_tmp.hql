 --当天访客次数
CREATE TABLE IF NOT EXISTS  tmp_uba_visit_sid
(visit_id string ,
visit_site string,
 cnt int) ;
    --访客次数
 insert overwrite table tmp_uba_visit_sid
select visit_id,visit_site, count(distinct session_id) as cnt
  from t_uba_log
 where dt = '${dayAgo}'
 and visit_site is not null
 group by visit_id,visit_site;