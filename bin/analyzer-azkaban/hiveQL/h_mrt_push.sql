

create table if not exists t_mrt_push (
	  push_biz_type String,
	  push_time String,
	  application_key String,
	  province_id String,
	  msg_type String,
	  push_type String,
	  cou String
  );
  
  -- 手厅推送统计
  INSERT OVERWRITE TABLE T_MRT_PUSH
  select 
  t.push_biz_type as PUSH_BIZ_TYPE,
  substr(t.push_time,0,8) as PUSH_TIME,
  t.channel_no as APPLICATION_KEY,
  t.prov_id as PROVINCE_ID,
  t.msg_type as MSG_TYPE,
  t.push_type as PUSH_TYPE,
  count(*) as COU  from t_ods_push t
  where t.dt='${hiveconf:dt}' 
  group by 
  t.push_biz_type,
  substr(t.push_time,0,8),
  t.channel_no,
  t.prov_id,
  t.msg_type,
  t.push_type;
