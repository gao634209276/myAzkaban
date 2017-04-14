CREATE TEMPORARY FUNCTION decode AS 'com.nexr.platform.hive.udf.GenericUDFDecode';
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.auto.convert.join=false;
insert into table t_ods_intf_detail partition(dt,chn)
SELECT   
obiv3.trans_id,
obiv3.auto_id,
obiv3.bss_id,
obiv3.user_mobile,
COALESCE(erp.area_name,obiv3.province_id),
COALESCE(erc.area_name,obiv3.city_id),
decode(obiv3.net_id,'01','2G','02','3G','03','固定电话','04','宽（ADSL）','05','宽带（LAN）','06','小灵通','07','WLAN业务','08','融合','09','集团','10','上网卡','11','4G','12','4G固定电话','13','4G宽带ADSL','14','4G宽带LAN','E','其它',obiv3.net_id),
decode(obiv3.pay_id,'0','无','1','预付费','2','后付费',obiv3.pay_id),
decode(obiv3.brand_id,'1','世界风','2','如意通','3','新势力','4','新时空','5','联通商务','6','亲情1+','7','其他','8','无线上网卡','9','沃','10','OCS','A','沃4G',obiv3.brand_id),
obiv3.provider_key,
decode(obiv3.application,'112000001','短信营业厅','113000001','触屏版','113000004','IPHONE 客户端版','113000005','Android 客户端','111000002','网上营业厅','113000002','标准版','111000023','网上营业厅','1110000023','网上营业厅',obiv3.application),
obiv3.version,
obiv3.interface_id,
COALESCE(db.business_name,obiv3.interface_name),
obiv3.interface_type,
decode(obiv3.interface_type,'1','查询类','2','交费组','3','办理组','4','销售组','5','信息组',obiv3.interface_type),
COALESCE(dr.result_name,obiv3.result,""),
obiv3.fail_reson,
obiv3.response_code,
obiv3.query_time,
obiv3.change_type,
obiv3.cur_package_id,
obiv3.cur_package_name,
obiv3.cur_package_type,
obiv3.cur_product_id,
obiv3.new_package_id,
obiv3.new_package_name,
obiv3.new_packaget_type,
obiv3.new_product_type,
decode(obiv3.operate_type,'1','查询','2','开通','3','关闭','4','变更',obiv3.operate_type),
decode(obiv3.commit_type,'1','查询','2','资格校验','3','预提交','4','提交',obiv3.commit_type),
obiv3.is_reback,
decode(obiv3.effect_type,'1','次月生效','2','立即生效',obiv3.effect_type),
"",
"",
obiv3.dt,
obiv3.application
FROM t_ods_biz_intf_v3 obiv3 
LEFT JOIN t_ebd_region erp 
ON(obiv3.province_id = erp.area_uniform_code AND erp. power_level = '1')
LEFT JOIN t_ebd_region erc 
ON(obiv3.city_id = erc.area_uniform_code AND erc. power_level = '2')
LEFT JOIN t_dimension_result dr 
ON dr.result_id = obiv3.result 
LEFT JOIN t_dimension_buss db 
ON db.business_code = obiv3.interface_id 
WHERE obiv3.dt='${hiveconf:dt}' AND obiv3.insert_time>'${hiveconf:curhour}';
