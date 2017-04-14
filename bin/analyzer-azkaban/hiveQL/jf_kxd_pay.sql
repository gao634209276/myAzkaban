select tradeglide,
       termid,
       orgid,
       tradetime,
       premoney,
       tradecode,
       tradedesp,
       remark,
       extdata1,
       extdata2,
       extdata3,
       provincecode,
       citycode,
       productname,
       papers,
       cardid,
       channeltype
  from t_ods_kxd_pay
 where dt = '${hiveconf:strdate}';

