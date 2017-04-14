set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.compress.output=true;
set hive.auto.convert.join=false;


create table if not exists spark_t_tmp_tg_level like t_tmp_tg_level;
truncate table spark_t_tmp_tg_level;
create table if not exists spark_t_tmp_f_mob_A like t_tmp_f_mob_A;
truncate table spark_t_tmp_f_mob_A;
--推广表 t_tmp_f_tg_new : dt_tg,tg_1m,tg_6m
--登录表  t_mid_mob_log  : dt
--推荐后一个月内有使用 并且 推荐前6个月内没有使用过 【Q-D】  A
 --Q:推荐后一个月内有使用
 --D:推荐前六个月内有使用
insert into table spark_t_tmp_f_mob_A
  select t1.mob, t1.dt_log, t1.dt_tg
    from (select mob, min(dt_log) dt_log, min(dt_tg) dt_tg
            from t_tmp_f_mob_mon
           where dt_log <= tg_1m
             and dt_log >= dt_tg
           group by mob) t1
    left join (select mob, min(dt_log) dt_log, min(dt_tg) dt_tg
                 from t_tmp_f_mob_mon
                where dt_log >=tg_6m and dt_log<dt_tg
                group by mob) t2 on t1.mob = t2.mob
   where t2.mob is null;

truncate table spark_t_tmp_tg_level partition(tg_level = 'C');
 --C  被推荐用户超过一个月了，但用户仍然未登录的，记录C  (C，D都符合 优先C)
insert overwrite table spark_t_tmp_tg_level partition(tg_level = 'C')
  select mob, max(dt_log), '', '', ''
    from t_tmp_f_mob_mon
   where dt_tg < '${monthAgo}'
     and dt_log =''
   group by mob;


--D  被推荐用户6个月内有登录客户端的记录，记录D   剔除C
insert overwrite table spark_t_tmp_tg_level partition(tg_level = 'D')
select t1.mob, t1.dt_log, '登录', '', ''
  from (select mob, max(dt_log) dt_log
          from t_tmp_f_mob_mon
         where dt_log >=tg_6m and dt_log<dt_tg
         group by mob) t1
  left join (select mob from spark_t_tmp_tg_level where tg_level = 'C') t2 on t1.mob = t2.mob
 where t2.mob is null;



 --A  登录  （1）登录客户端（A积分奖励） 推荐前6个月内没有使用手厅，并且推荐后一个月内在手厅有如下行为则级别为对应的等级
insert overwrite table spark_t_tmp_tg_level partition(tg_level='A')
select  mob,dt_log,'登录','','' from spark_t_tmp_f_mob_A;

--3A   流量包订购  合约类商品订购  使用客户端订购合约类商品或办理流量包（3A积分奖励）


insert overwrite table spark_t_tmp_tg_level partition
  (tg_level = '3A')
  select t1.mob, t2.order_time, '合约类商品订购', '', ''
    from spark_t_tmp_f_mob_A t1
    join t_ods_scmsg_tg t2 on t1.mob = t2.user_mobile
   where substr(t2.order_time, 0, 8) >=t1.dt_tg ;

insert overwrite table spark_t_tmp_tg_level partition(tg_level = '3D')
  select t1.mob, t2.order_time, '合约类商品订购', '', ''
    from (select mob from spark_t_tmp_tg_level where tg_level = 'D') t1
    join t_ods_scmsg_tg t2 on t1.mob = t2.user_mobile;



 insert into table spark_t_tmp_tg_level partition(tg_level = '3A')
   select t1.mob, t2.query_time, '流量包订购', result, fail_reson
     from spark_t_tmp_f_mob_A t1
     join (select user_mobile, query_time, result, fail_reson
             from t_mid_mob_llb ) t2 on t1.mob = t2.user_mobile
    where substr(t2.query_time, 0, 8)>=t1.dt_tg;

   insert into table spark_t_tmp_tg_level partition(tg_level = '3D')
   select t1.mob, t2.query_time, '流量包订购', result, fail_reson
     from (select mob from spark_t_tmp_tg_level where tg_level = 'D') t1
     join (select user_mobile, query_time, result, fail_reson
             from t_mid_mob_llb ) t2 on t1.mob = t2.user_mobile;


--2A  办理业务  交费充值  使用客户端交费/办理任意业务（2A积分奖励）
--办理业务 剔除流量包用户
insert overwrite table spark_t_tmp_tg_level partition(tg_level='2A')
  select t1.user_mobile, t1.query_time, '办理业务', '', ''
    from (select user_mobile, query_time
            from t_mid_tg_f_mob_intf) t1
    join spark_t_tmp_f_mob_A t2 on t2.mob = t1.user_mobile
  left join(
    select distinct mob
      from spark_t_tmp_tg_level
     where tg_level = '3A'
       and falg_visit = '流量包订购') t3 on t1.user_mobile = t3.mob
     where t3.mob is null  and substr(query_time,0,8)>=t2.dt_tg;


 insert overwrite table spark_t_tmp_tg_level partition(tg_level='2D')
  select t1.user_mobile, t1.query_time, '办理业务', '', ''
    from (select user_mobile, query_time
            from t_mid_tg_f_mob_intf ) t1
    join (select mob from spark_t_tmp_tg_level where tg_level = 'D') t2 on t2.mob = t1.user_mobile
  left join(
    select distinct mob
      from spark_t_tmp_tg_level
     where tg_level = '3D'
       and falg_visit = '流量包订购') t3 on t1.user_mobile = t3.mob
     where t3.mob is null;


insert into table spark_t_tmp_tg_level partition(tg_level='2A')
select t1.mob, t2.dt, '交费充值','',''
  from spark_t_tmp_f_mob_A t1
  join (select user_mobile mob, payment_time dt
          from t_ods_jfmsg_tg ) t2 on t1.mob = t2.mob
 where  substr(t2.dt, 0, 8)>=t1.dt_tg;

insert into table spark_t_tmp_tg_level partition(tg_level='2D') select t1.mob, t2.dt, '交费充值','','' from (select mob from spark_t_tmp_tg_level where tg_level = 'D') t1 join (select user_mobile mob, payment_time dt from t_ods_jfmsg_tg) t2 on t1.mob = t2.mob;