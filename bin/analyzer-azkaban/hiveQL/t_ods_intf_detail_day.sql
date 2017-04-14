set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.merge.mapredfiles=true;
set hive.exec.reducers.bytes.per.reducer=1000000000;
insert overwrite table t_ods_intf_detail partition(dt,chn)
SELECT   
*
FROM t_ods_intf_detail obiv3  
WHERE obiv3.dt='${hiveconf:d3dt}';
