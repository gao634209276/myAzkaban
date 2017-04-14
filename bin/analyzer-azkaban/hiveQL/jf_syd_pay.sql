select buss_id,
       buss_state,
       pro_id,
       city_id,
       store_type,
       package_name,
       net_id,
       brand_id,
       active_type,
       paid_in,
       receive_account,
       pay_state,
       pay_type,
       pay_time,
       user_type,
       user_name,
       card_type,
       card_number,
       card_address,
       shop_number,
       buss_time,
       open_channel,
       development_ess,
       development_name,
       development_code,
       mini_bssid,
       psam_id,
       channel
  from t_ods_syd_pay
   where dt = '${hiveconf:strdate}';

