
 -- 手厅业务喜好结果表
create table if not exists t_mrt_fav  (
   query_time       	String,
   biz_id               String,
   prov_id              String,
   channel_id           String,
   is_fav               String,
   cou                  String
);

INSERT OVERWRITE TABLE t_mrt_fav
select 
t.dt as QUERY_TIME,
t.biz_id as BIZ_ID,
t.prov_id as PROV_ID,
t.channel_id as CHANNEL_ID,
t.is_fav as IS_FAV,
count(*) as cou
from t_ods_fav t 
 where  t.dt='${hiveconf:dt}'
group by 
t.dt,
t.biz_id,
t.prov_id,
t.channel_id,
t.is_fav;



