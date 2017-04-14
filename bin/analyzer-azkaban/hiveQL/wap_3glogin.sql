 select distinct concat( user_mobile,
                   '|',
                   province_id,
                   '|',
                   city_id,
                   '|',
                   islogin)
       from t_mid_detail_jf2g3g
      where province_id = '${hiveconf:pid}'
        and mon = '${hiveconf:dt}';

