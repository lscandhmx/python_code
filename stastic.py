/*
目的：众悦看板--统计表
频次：每日
时间：2020-08-12
分区：动态分区
*/

/*设置动态分区*/
SET hive.exec.dynamic.partition=true; --使用动态分区
Set hive.exec.dynamic.partition.mode=nonstrict;--无限制模式
SET hive.exec.max.dynamic.partitions.pernode = 1000;--每个节点生成动态分区最大个数
SET hive.exec.max.dynamic.partitions=1000;--一个任务最多可以创建的文件数目


insert overwrite table cc_ads.zhongyue_abnormal_board_all_driver_num_1d partition(dt)

---------------------------------------------union all-----------------------------------------------
-------------------------------------count offline driver num----------------------------------
select coalesce(lease_name,'未知') as lease_name,second_lease_name,-1 as car_service_type,count(driver_no) as driver_num,1 as tabel_options,
concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) as day_key,lease_id,second_lease_no,dt
from cc_ads.zhongyue_abnormal_board_offline_driver_1d2 where dt=regexp_replace(date_sub('${current_date}',1),'-','') and cycle_type=1
group by lease_name,second_lease_name,lease_id,second_lease_no,dt
union all
select coalesce(lease_name,'未知') as lease_name,second_lease_name,coalesce(car_service_type,0) as car_service_type,count(driver_no) as driver_num,1 as tabel_options,
concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) as day_key,lease_id,second_lease_no,dt
from cc_ads.zhongyue_abnormal_board_offline_driver_1d2  where dt=regexp_replace(date_sub('${current_date}',1),'-','') and cycle_type=1
group by lease_name,second_lease_name,lease_id,second_lease_no,car_service_type,dt
union all
-------------------------------count unfinished order driver num--------------------------
select coalesce(lease_name,'未知') as lease_name,second_lease_name,-1 as car_service_type,count(driver_no) as driver_num,2 as tabel_options,
concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) as day_key,lease_id,second_lease_no,dt
from cc_ads.zhongyue_abnormal_board_unfinished_order_driver_1d where dt=regexp_replace(date_sub('${current_date}',1),'-','') and cycle_type=1
group by lease_name,second_lease_name,lease_id,second_lease_no,dt
union all
select coalesce(lease_name,'未知') as lease_name,second_lease_name,coalesce(car_service_type,0) as car_service_type,count(driver_no) as driver_num,2 as tabel_options,
concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) as day_key,lease_id,second_lease_no,dt
from cc_ads.zhongyue_abnormal_board_unfinished_order_driver_1d  where dt=regexp_replace(date_sub('${current_date}',1),'-','') and cycle_type=1
group by lease_name,second_lease_name,lease_id,second_lease_no,car_service_type,dt
union all
-------------------------------count order_abnormal driver num--------------------------
select coalesce(lease_name,'未知') as lease_name,second_lease_name,-1 as car_service_type,count(driver_no) as driver_num,3 as tabel_options,
concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) as day_key,lease_id,second_lease_no,dt
from cc_ads.zhongyue_abnormal_board_order_abnormal_driver_1d where dt=regexp_replace(date_sub('${current_date}',1),'-','') and cycle_type=1
group by lease_name,second_lease_name,lease_id,second_lease_no,dt
union all
select coalesce(lease_name,'未知') as lease_name,second_lease_name,coalesce(car_service_type,0) as car_service_type,count(driver_no) as driver_num,3 as tabel_options,
concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) as day_key,lease_id,second_lease_no,dt
from cc_ads.zhongyue_abnormal_board_order_abnormal_driver_1d  where dt=regexp_replace(date_sub('${current_date}',1),'-','') and cycle_type=1
group by lease_name,second_lease_name,car_service_type,lease_id,second_lease_no,dt
union all
-------------------------------count Complaint driver num--------------------------
select coalesce(lease_name,'未知') as lease_name,second_lease_name,-1 as car_service_type,count(driver_no) as driver_num,4 as tabel_options,
concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) as day_key,lease_id,second_lease_no,dt
from cc_ads.zhongyue_abnormal_board_complaint_driver_1d where dt=regexp_replace(date_sub('${current_date}',1),'-','') and cycle_type=1
group by lease_name,second_lease_name,lease_id,second_lease_no,dt
union all
select coalesce(lease_name,'未知') as lease_name,second_lease_name,coalesce(car_service_type,0) as car_service_type,count(driver_no) as driver_num,4 as tabel_options,
concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) as day_key,lease_id,second_lease_no,dt
from cc_ads.zhongyue_abnormal_board_complaint_driver_1d  where dt=regexp_replace(date_sub('${current_date}',1),'-','') and cycle_type=1
group by lease_name,second_lease_name,lease_id,second_lease_no,car_service_type,dt
union all
-------------------------------count risk order driver num--------------------------
--count risk_order driver num include last_last_week to yesterday
select coalesce(lease_name,'未知') as lease_name,second_lease_name,-1 as car_service_type,count(driver_no) as driver_num,5 as tabel_options,day_key,
lease_id,second_lease_no,regexp_replace(date_sub('${current_date}',1),'-','') as dt
from
(select risk_order.driver_no,driver_name,driver_phone,lease_name,second_lease_name,lease_id,second_lease_no,car_service_type,count(order_no) as order_num,day_key from
(select driver_no, order_no,substr(use_time,1,10) as day_key,1 as ro from cc_dw.t_dwd__penalty__penalty_order__f where dt=regexp_replace(date_sub('${current_date}',1),'-','')
and substr(use_time,1,10) between date_sub(next_day('${current_date}','MO'),21) and date_sub('${current_date}',1)
) as risk_order
left join
(select driver_no as driver_no_1,driver_name,driver_phone,lease_name,second_lease_name,lease_id,second_lease_no,coalesce(car_service_type,0) as car_service_type from cc_dw.t_dwd_pub_cp_driver_job_info_1d
where dt=regexp_replace(date_sub('${current_date}',1),'-',''))as driver
on risk_order.driver_no=driver.driver_no_1
where driver_no_1 is not null
group by risk_order.driver_no,driver_name,driver_phone,lease_name,second_lease_name,lease_id,second_lease_no,car_service_type,day_key)
group by lease_name,second_lease_name,lease_id,second_lease_no,day_key
union all
select coalesce(lease_name,'未知') as lease_name,second_lease_name,coalesce(car_service_type,0) as car_service_type,count(driver_no) as driver_num,5 as tabel_options,day_key,
lease_id,second_lease_no,regexp_replace(date_sub('${current_date}',1),'-','') as dt
from
(select risk_order.driver_no,driver_name,driver_phone,lease_name,second_lease_name,lease_id,second_lease_no,car_service_type,count(order_no) as order_num,day_key from
(select driver_no, order_no,substr(use_time,1,10) as day_key,1 as ro from cc_dw.t_dwd__penalty__penalty_order__f where dt=regexp_replace(date_sub('${current_date}',1),'-','')
and substr(use_time,1,10) between date_sub(next_day('${current_date}','MO'),21) and date_sub('${current_date}',1)
) as risk_order
left join
(select driver_no as driver_no_1,driver_name,driver_phone,lease_name,second_lease_name,lease_id,second_lease_no,coalesce(car_service_type,0) as car_service_type from cc_dw.t_dwd_pub_cp_driver_job_info_1d
where dt=regexp_replace(date_sub('${current_date}',1),'-',''))as driver
on risk_order.driver_no=driver.driver_no_1
where driver_no_1 is not null
group by risk_order.driver_no,driver_name,driver_phone,lease_name,second_lease_name,lease_id,second_lease_no,car_service_type,day_key)
group by lease_name,second_lease_name,lease_id,second_lease_no,car_service_type,day_key
union all
-------------------------------count cancel_order driver num--------------------------
select coalesce(lease_name,'未知') as lease_name,second_lease_name,-1 as car_service_type,count(driver_no) as driver_num,6 as tabel_options,
concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) as day_key,lease_id,second_lease_no,dt
from cc_ads.zhongyue_abnormal_board_cancel_order_driver_1d where dt=regexp_replace(date_sub('${current_date}',1),'-','') and cycle_type=1
group by lease_name,second_lease_name,lease_id,second_lease_no,dt
union all
select coalesce(lease_name,'未知') as lease_name,second_lease_name,coalesce(car_service_type,0) as car_service_type,count(driver_no) as driver_num,6 as tabel_options,
concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) as day_key,lease_id,second_lease_no,dt
from cc_ads.zhongyue_abnormal_board_cancel_order_driver_1d  where dt=regexp_replace(date_sub('${current_date}',1),'-','') and cycle_type=1
group by lease_name,second_lease_name,car_service_type,lease_id,second_lease_no,dt


DISTRIBUTE BY dt;