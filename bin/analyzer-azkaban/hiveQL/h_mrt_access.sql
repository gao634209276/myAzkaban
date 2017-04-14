
-- h_mrt_access.sql nginx访问日志统计sql
from t_nginx_base insert overwrite table t_mid_access
 select 
 channel_no,
 concat('0',prov_id),
 city_id,
 time_key,
 "iservice.10010.com",
 version,
 count(req_url),
 count(distinct uvst),
 count(distinct client_ip),
 count(distinct usst)
where 
 dt='${hiveconf:dt}' and substr(query_time,0,10)='${hiveconf:hour}'
 and req_url not like '%websitetotalservice%' 
group by channel_no,
		 prov_id,
		 city_id,
		 time_key,
		 version;
		 
-- 全渠道访问日志统计		 
from t_all_access_base insert into  table t_mid_access
select 
channel_no,
if(source_prov!='',source_prov,'N/A') prov,
if(source_city!='',source_prov,'N/A') city,
time_key,
parse_url(page_url,"HOST") url,
"1",
count(parse_url(page_url,"HOST")) pv,
0,
count(distinct client_ip) ip,
0
where 
 channel_no!='115000001' and  channel_no!='114000001' and
dt='${hiveconf:dt}' and substr(query_time,0,10)='${hiveconf:hour}'
group by channel_no,
		 source_prov,
		 source_city,
		 time_key,
		 parse_url(page_url,"HOST");

