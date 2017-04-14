set hive.exec.compress.output=true;

-- 产品用户分析 新增用户
-- 使用用户 USER
-- 新增用户 ADD
-- 累计用户 TOTAL

 -- 使用用户 USER
insert OVERWRITE table T_MRT_LOALY_USER  PARTITION(mon= '${hiveconf:dt}',user_type='USER')
select province_id,
       city_id,
       application_key,
       user_type_key,
       pay_type_key ,count(distinct user_mobile)
  from T_MID_LOALY_USER
 where mon =  '${hiveconf:dt}'
 group by province_id,
          city_id,
          application_key,
          user_type_key,
          pay_type_key;

insert into table T_MRT_LOALY_USER PARTITION
  (mon =  '${hiveconf:dt}', user_type = 'USER')
  select province_id,
         city_id,
         'all',
         user_type_key,
         pay_type_key,
         count(distinct user_mobile)
    from T_MID_LOALY_USER
   where mon =  '${hiveconf:dt}'
   group by province_id, city_id, user_type_key, pay_type_key;
 -- 新增用户 ADD
 insert OVERWRITE table T_MRT_LOALY_USER  PARTITION(mon= '${hiveconf:dt}',user_type='ADD')
select province_id,
       city_id,
       application_key,
       user_type_key,
       pay_type_key ,count(distinct user_mobile)
  from T_MID_CURADD_USER
 where mon = '${hiveconf:dt}'
 group by province_id,
          city_id,
          application_key,
          user_type_key,
          pay_type_key;
		  		  
-- 累计用户 TOTAL
insert OVERWRITE table T_MRT_LOALY_USER  PARTITION(mon= '${hiveconf:dt}',user_type='TOTAL')
select province_id,
       city_id,
       application_key,
       user_type_key,
       pay_type_key ,count(distinct user_mobile)
  from T_MID_LOALY_USER
 where mon <=  '${hiveconf:dt}'
 group by province_id,
          city_id,
          application_key,
          user_type_key,
          pay_type_key;

insert into table T_MRT_LOALY_USER PARTITION
  (mon = '${hiveconf:dt}', user_type = 'TOTAL')
  select province_id,
         city_id,
         'all',
         user_type_key,
         pay_type_key,
         count(distinct user_mobile)
    from T_MID_LOALY_USER
   where mon <=  '${hiveconf:dt}'
   group by province_id, city_id, user_type_key, pay_type_key;
