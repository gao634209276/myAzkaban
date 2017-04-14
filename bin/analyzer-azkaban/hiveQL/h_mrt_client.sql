 create table if not exists t_mrt_client (
 query_time string,
 prov_id string,
 net_type string,
 brand string,
 model string,
 os string,
 version string,
 cou bigint,
 u_cou bigint
  );

  -- 手厅客户端启动分析
INSERT OVERWRITE TABLE T_MRT_CLIENT 
  select 
  substr(open_app_time,0,8) as QUERY_TIME,
  '',
  net_type as NET_TYPE,
  brand as BRAND,
  model as MODEL,
  os as OS,
  version as VERSION,
  count(*) as COU,
  count(distinct imie) as U_COU
  from t_ods_client t
  where dt='${hiveconf:dt}'
  group by 
  substr(open_app_time,0,8),
  net_type,
  brand,
  model,
  os,
  version;



