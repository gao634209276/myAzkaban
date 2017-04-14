set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.compress.output=true;
set  hive.auto.convert.join=false;
CREATE TEMPORARY FUNCTION str_to_date AS 'com.nexr.platform.hive.udf.UDFStrToDate';
CREATE TEMPORARY FUNCTION decode AS 'com.nexr.platform.hive.udf.GenericUDFDecode';


insert overwrite table t_codewapdetail_dt partition (dt='${dayAgo}')
select t1.province_code,
         t1.city_code,
         t1.user_mobile_num,
         t3.num_id,
         t1.business_hall_name,
         decode(t1.channel_id, 'wolm_tg', '沃联盟', '码上购'),
         t1.business_hall_people_name,
         t1.register_time,
         case
           when length(dt_visit) > 10 then
            str_to_date(dt_visit, 'yyyyMMddHHmmss')
           when length(dt_visit) = 8 then
            str_to_date(dt_visit, 'yyyyMMdd')
           else
            dt_visit
         end,
       case
         when t2.tg_level is null then
          decode(t1.flow_flag, '1', 'A', '2', 'A', t2.tg_level)
         else
          t2.tg_level
       end,
       case
         when t2.falg_visit is null then
          decode(t1.flow_flag, '1', '登录', '2', '登录', t2.falg_visit)
         else
          t2.falg_visit
       end
         ,
         '',
        case
         when (tg_level is null or tg_level = 'C') and
              t1.flow_flag not in ('1', '2') then
          '推荐状态'
         when tg_level = 'A' or tg_level = 'D' or t1.flow_flag in ('1', '2') then
          '用户登录'
         when tg_level = '2A' or tg_level = '3A' or tg_level = '2D' or
              tg_level = '3D' then
          '办理业务'
       end,
         decode(t1.flow_flag,
                '0',
                '未领取',
                '1',
                '成功',
                '2',
                '失败',
                t1.flow_flag),
         decode(t1.remark1,'null','',t1.remark1),
         nvl(t3.dept_name, ''),
         nvl(t3.staff_name, '')
    from (select * from t_tui_guang_client where dt = '${dayAgo}') t1
    left join t_tmp_tg_level t2 on t1.user_mobile_num = t2.mob
    left join (select developer_id,
                      max(num_id) num_id,
                      max(dept_name) dept_name,
                      max(staff_name) staff_name
                 from t_mob_tuiguang_develop
                group by developer_id) t3 on t1.business_hall_people_name =
                                             t3.developer_id;
  
insert overwrite table t_codewapdetail
  select province_id,
         city_id,
         usermobile,
         fzr_usermobile,
         application,
         app_from,
         fazhanren,
         tuijian_time,
         activ_time,
         activ_level,
         activ_type,
         product_name,
         state_type,
         lq_result,
         fail_respon,yyt_no,develop_no
    from t_codewapdetail_dt
   where (substr(regexp_replace(activ_time, '-', ''), 0, 6) =
         substr('${dayAgo}', 0, 6) or
         substr(regexp_replace(tuijian_time, '-', ''), 0, 6) >=
         substr('${dayAgo}', 0, 6))
     and dt = '${dayAgo}';
