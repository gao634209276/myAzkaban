set hive.exec.compress.output=true;

-- h_mid_mon_user.sql产品用户分析 新增用户
 insert OVERWRITE table T_MID_CURADD_USER PARTITION
    (mon = '${hiveconf:dt}')
    select user_mobile,
           pay_type_key,
           user_type_key,
           application_key,
           province_id,
           city_id
      from (select user_mobile,
                   pay_type_key,
                   user_type_key,
                   application_key,
                   province_id,
                   city_id,
                   max(mon) mon,count(1) cou
              from T_MID_LOALY_USER
             where mon <= '${hiveconf:dt}'
             group by user_mobile,
                      pay_type_key,
                      user_type_key,
                      application_key,
                      province_id,
                      city_id) t
     where mon ='${hiveconf:dt}'  and cou='1';
  --全渠道新增用户数
 insert into table T_MID_CURADD_USER PARTITION
    (mon = '${hiveconf:dt}')
    select user_mobile,
           pay_type_key,
           user_type_key,
           'all',
           province_id,
           city_id
      from (select user_mobile,
                   pay_type_key,
                   user_type_key,
                    province_id,
                   city_id,
                   max(mon) mon,count(1) cou
              from T_MID_LOALY_USER
             where mon <= '${hiveconf:dt}'
             group by user_mobile,
                      pay_type_key,
                      user_type_key,
                      province_id,
                      city_id) t
     where mon = '${hiveconf:dt}'and cou='1';