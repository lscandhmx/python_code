/*
目的：众悦看板--未上线司机
频次：每日
时间：2020-08-12
分区：动态分区
*/

/*设置动态分区*/
SET hive.exec.dynamic.partition=true; --使用动态分区
Set hive.exec.dynamic.partition.mode=nonstrict;--无限制模式
SET hive.exec.max.dynamic.partitions.pernode = 1000;--每个节点生成动态分区最大个数
SET hive.exec.max.dynamic.partitions=1000;--一个任务最多可以创建的文件数目


--Operable_driver_online_time include day/week/month
with Operable_driver_online_time as
(
select Operable_driver.driver_no,driver_name,driver_phone,lease_name,second_lease_name,lease_id,second_lease_no,car_service_type,
coalesce(yesterday_online_time,0) as yesterday_online_time,coalesce(before_yesterday_online_time,0) as before_yesterday_online_time,
coalesce(last_week_online_time,0) as last_week_online_time,coalesce(last_last_week_online_time/last_last_week_online_day,0) as last_last_week_online_time,
coalesce(last_month_online_time,0) as last_month_online_time,coalesce(last_last_month_online_time/last_last_month_online_day,0) as last_last_month_online_time
from
(
--select Operable_driver's online time
select Operable_driver.driver_no,driver_name,driver_phone,lease_name,second_lease_name,lease_id,second_lease_no,car_service_type,
-- yesterday_online_time by day
sum(case when last_time=date_sub('${current_date}',1) then last_total_time end)/3600 as yesterday_online_time,
--before_yesterday_online_time
sum(case when last_time=date_sub('${current_date}',2) then last_total_time end)/3600 as before_yesterday_online_time,
--last_week_online_time
sum(case when last_time between date_sub(next_day('${current_date}','MO'),14) and date_sub(next_day('${current_date}','MO'),8) then last_total_time end)/3600
as last_week_online_time,
--last_last_week_online_time
sum(case when last_time between date_sub(next_day('${current_date}','MO'),21) and date_sub(next_day('${current_date}','MO'),15) then last_total_time end)/3600
as last_last_week_online_time,
--count last_last_week_online_day
count(case when last_time between date_sub(next_day('${current_date}','MO'),21) and date_sub(next_day('${current_date}','MO'),15) then Operable_driver.driver_no end)
as last_last_week_online_day,
--last_month_online_time
sum(case when last_time between trunc(add_months('${current_date}',-1),'MM') and date_sub(trunc('${current_date}','MM'),1) then last_total_time end)/3600
as last_month_online_time,
--last_last_month_online_time
sum(case when last_time between trunc(add_months('${current_date}',-2),'MM') and date_sub(trunc(trunc(add_months('${current_date}',-1),'MM'),'MM'),1) then last_total_time end)/3600
as last_last_month_online_time,
--count last_last_month_online_day
count(case when last_time between trunc(add_months('${current_date}',-2),'MM') and date_sub(trunc(trunc(add_months('${current_date}',-1),'MM'),'MM'),1) then Operable_driver.driver_no end)
as last_last_month_online_day
from
(
select * from
--select Operable driver
(select driver_no,driver_name,driver_phone,lease_name,second_lease_name,lease_id,second_lease_no,coalesce(car_service_type,0) as car_service_type from cc_dw.t_dwd_pub_cp_driver_job_info_1d
where dt=regexp_replace(date_sub('${current_date}',1),'-','')
and driver_status in(12,13,15)) as Operable_driver
--join the time between last_last_month1 and yesterday
left join
(
select driver_no,total_time as last_total_time,concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) as last_time from cc_dw.t_fct_driver_online
where concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) between trunc(add_months('${current_date}',-2),'MM') and date_sub('${current_date}',1)
) as last_o_t
on Operable_driver.driver_no=last_o_t.driver_no
)
group by Operable_driver.driver_no,driver_name,driver_phone,lease_name,second_lease_name,lease_id,second_lease_no,car_service_type
)
)

insert overwrite table cc_ads.zhongyue_abnormal_board_offline_driver_1d2 partition(dt)

------------union all-------------------
--select offline_driver_d
select driver_no,driver_name,driver_phone,lease_name,second_lease_name,car_service_type,
yesterday_online_time as last_cycle_online_time,before_yesterday_online_time as last_last_cycle_online_time,1 as cycle_type,
lease_id,second_lease_no,regexp_replace(date_sub('${current_date}',1),'-','') as dt
from Operable_driver_online_time where yesterday_online_time=0
union all
(--select offline_driver_w
select driver_no,driver_name,driver_phone,lease_name,second_lease_name,car_service_type,
last_week_online_time as last_cycle_online_time,last_last_week_online_time as last_last_cycle_online_time,2 as cycle_type,
lease_id,second_lease_no,regexp_replace(date_sub('${current_date}',1),'-','') as dt
from Operable_driver_online_time where last_week_online_time=0
)
union all
(--select offline_driver_m
select driver_no,driver_name,driver_phone,lease_name,second_lease_name,car_service_type,
last_month_online_time as last_cycle_online_time,last_last_month_online_time as last_last_cycle_online_time,3 as cycle_type,
lease_id,second_lease_no,regexp_replace(date_sub('${current_date}',1),'-','') as dt
from Operable_driver_online_time where last_month_online_time=0
)

DISTRIBUTE BY dt;