 --��ʧ�û� ������ 
 insert OVERWRITE table T_MRT_LOALY_USER  PARTITION(mon= '${hiveconf:dt}',user_type='LOSE')
select province_id,
       city_id,
       application_key,
       user_type_key,
       pay_type_key,
       count(distinct user_mobile)
  from (select user_mobile,
               pay_type_key,
               user_type_key,
               application_key,
               province_id,
               city_id,
               min(mon) mon,
               count(1) cou
          from T_MID_LOALY_USER
         where mon >= '${hiveconf:dt_7}'
           and mon <= '${hiveconf:dt}'
         group by user_mobile,
                  pay_type_key,
                  user_type_key,
                  application_key,
                  province_id,
                  city_id) t
 where mon = '${hiveconf:dt_7}'
   and cou = '1'
 group by province_id,
          city_id,
          application_key,
          user_type_key,
          pay_type_key;
 --��ʧ�û� ȫ���� 
 insert into table T_MRT_LOALY_USER  PARTITION(mon= '${hiveconf:dt}',user_type='LOSE')
select province_id,
       city_id,
       'all',
       user_type_key,
       pay_type_key,
       count(distinct user_mobile)
  from (select user_mobile,
               pay_type_key,
               user_type_key,
                province_id,
               city_id,
               min(mon) mon,
               count(1) cou
          from T_MID_LOALY_USER
         where mon >= '${hiveconf:dt_7}'
           and mon <= '${hiveconf:dt}'
         group by user_mobile,
                  pay_type_key,
                  user_type_key,
                   province_id,
                  city_id) t
 where mon = '${hiveconf:dt_7}'
   and cou = '1'
 group by province_id,
          city_id,
            user_type_key,
          pay_type_key;
		  
--��Ĭ�û� ������ 
 insert OVERWRITE table T_MRT_LOALY_USER  PARTITION(mon= '${hiveconf:dt}',user_type='SINLENCE')
select province_id,
       city_id,
       application_key,
       user_type_key,
       pay_type_key,
       count(distinct user_mobile)
  from (select user_mobile,
               pay_type_key,
               user_type_key,
               application_key,
               province_id,
               city_id,
               min(mon) mon,
               count(1) cou
          from T_MID_LOALY_USER
         where mon >= '${hiveconf:dt_4}'
           and mon <= '${hiveconf:dt}'
         group by user_mobile,
                  pay_type_key,
                  user_type_key,
                  application_key,
                  province_id,
                  city_id) t
 where mon = '${hiveconf:dt_4}'
   and cou = '1'
 group by province_id,
          city_id,
          application_key,
          user_type_key,
          pay_type_key;
 --��Ĭ�û� ȫ���� 
 insert into table T_MRT_LOALY_USER  PARTITION(mon= '${hiveconf:dt}',user_type='SINLENCE')
select province_id,
       city_id,
       'all',
       user_type_key,
       pay_type_key,
       count(distinct user_mobile)
  from (select user_mobile,
               pay_type_key,
               user_type_key,
                province_id,
               city_id,
               min(mon) mon,
               count(1) cou
          from T_MID_LOALY_USER
         where mon >= '${hiveconf:dt_4}'
           and mon <= '${hiveconf:dt}'
         group by user_mobile,
                  pay_type_key,
                  user_type_key,
                   province_id,
                  city_id) t
 where mon = '${hiveconf:dt_4}'
   and cou = '1'
 group by province_id,
          city_id,
            user_type_key,
          pay_type_key;