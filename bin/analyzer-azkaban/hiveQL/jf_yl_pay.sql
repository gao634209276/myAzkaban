select payorderid,
       serviceorderid,
       paychannelorder,
       payproviderorder,
       payorgcode,
       channelcode,
       connectchannel,
       paystatus,
       backtag,
       paycompletetime,
       merchantsid,
       refundtime,
       reserver1,
       paypoundage
  from t_ods_yl_pay 
   where dt='${hiveconf:strdate}';
