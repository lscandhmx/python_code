/*
目的：众悦看板--有责取消
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
with Operable_driver_online_time_simple as
(
--select Operable_driver's online time
select Operable_driver.driver_no,driver_name,driver_phone,lease_name,second_lease_name,lease_id,second_lease_no,car_service_type,
-- yesterday_online_time by day
sum(case when last_time=date_sub('${current_date}',1) then last_total_time end) as yesterday_online_time,
--last_week_online_time
sum(case when last_time between date_sub(next_day('${current_date}','MO'),14) and date_sub(next_day('${current_date}','MO'),8) then last_total_time end) as last_week_online_time,
--last_month_online_time
sum(case when last_time between trunc(add_months('${current_date}',-1),'MM') and date_sub(trunc('${current_date}','MM'),1) then last_total_time end) as last_month_online_time
from
--select Operable driver
(select driver_no,driver_name,driver_phone,lease_name,second_lease_name,lease_id,second_lease_no,coalesce(car_service_type,0) as car_service_type from cc_dw.t_dwd_pub_cp_driver_job_info_1d
where dt=regexp_replace(date_sub('${current_date}',1),'-','')
and driver_status in(12,13,15)) as Operable_driver
--join the time between last_last_month1 and yesterday
left join
(
select driver_no,total_time as last_total_time,concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) as last_time from cc_dw.t_fct_driver_online
where concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) between trunc(add_months('${current_date}',-1),'MM') and date_sub('${current_date}',1)
) as last_o_t
on Operable_driver.driver_no=last_o_t.driver_no
group by Operable_driver.driver_no,driver_name,driver_phone,lease_name,second_lease_name,lease_id,second_lease_no,car_service_type
)
--select all_revoke_order_driver_with_order_num include d/w/m
,driver_revoke_order_num as
(
select driver_no,count(order_no) as order_num,last_time from
(
--select all_order from zhongyue_driver
select driver_no,order_no,concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) as last_time from
(
(select order_no,driver_no,dt from cc_dw.t_dwd_pub_order_univ_use_time_1d2
where concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) between trunc(add_months('${current_date}',-2),'MM') and date_sub('${current_date}',1)
and biz=1 and who_revoke=2)
UNION all
(select order_no,driver_no,dt from cc_dw.t_dwd_pub_order_univ_use_time_1d2
where concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) between trunc(add_months('${current_date}',-2),'MM') and date_sub('${current_date}',1)
and biz=1 and revoke_code in (3,303))
UNION all
(select order_no,driver_no,dt from cc_dw.t_dwd_pub_order_univ_use_time_1d2
where concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) between trunc(add_months('${current_date}',-2),'MM') and date_sub('${current_date}',1)
and biz=2 and revoke_code in (3,303))
UNION all
(select boc_order__order_no as order_no,boc_order__driver_no as driver_no,dt
from cc_dw.t_dwd_boc_zhongyue_use_time_1d
where concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) between trunc(add_months('${current_date}',-2),'MM') and date_sub('${current_date}',1)
and boc_order__order_status=4 and boc_operation__operator_type=2)
UNION all
(select boc_order__order_no as order_no,boc_order__driver_no as driver_no,dt
from cc_dw.t_dwd_boc_zhongyue_use_time_1d
where concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) between trunc(add_months('${current_date}',-2),'MM') and date_sub('${current_date}',1)
and boc_operation__operation_log_revoketypecode in (3,303))
)
)
group by driver_no,last_time
)

insert overwrite table cc_ads.zhongyue_abnormal_board_cancel_order_driver_1d partition(dt)

---------------------unoin all---------------------------
--select revoke_order_driver_d
select driver_no,driver_name,driver_phone,lease_name,second_lease_name,car_service_type,
yesterday_revoke_order_num as last_cycle_revoke_order_num,before_yesterday_revoke_order_num as last_last_cycle_revoke_order_num,1 as cycle_type,
lease_id,second_lease_no,regexp_replace(date_sub('${current_date}',1),'-','') as dt
from
(
select yesterday_online_driver.driver_no,driver_name,driver_phone,lease_name,second_lease_name,lease_id,second_lease_no,car_service_type,
-- yesterday_revoke_order_num
coalesce(sum(case when last_time=date_sub('${current_date}',1) then order_num end),0) as yesterday_revoke_order_num,
--before_yesterday_revoke_order_num
coalesce(sum(case when last_time=date_sub('${current_date}',2) then order_num end),0) as before_yesterday_revoke_order_num
from
(--select online_driver by day
select driver_no,driver_name,driver_phone,lease_name,second_lease_name,lease_id,second_lease_no,car_service_type
from Operable_driver_online_time_simple where yesterday_online_time is not null
) as yesterday_online_driver
left join  --join online_driver and all_finish_order for order_num
(select driver_no,order_num,last_time from driver_revoke_order_num)
on yesterday_online_driver.driver_no=driver_revoke_order_num.driver_no
group by yesterday_online_driver.driver_no,driver_name,driver_phone,lease_name,second_lease_name,lease_id,second_lease_no,car_service_type
) where yesterday_revoke_order_num>0
union all
( --select revoke_driver_w
select driver_no,driver_name,driver_phone,lease_name,second_lease_name,car_service_type,
last_week_revoke_order_num as last_cycle_revoke_order_num,last_last_week_revoke_order_num as last_last_cycle_revoke_order_num,2 as cycle_type,
lease_id,second_lease_no,regexp_replace(date_sub('${current_date}',1),'-','') as dt
from
(
select last_week_online_driver.driver_no,driver_name,driver_phone,lease_name,second_lease_name,lease_id,second_lease_no,car_service_type,
--last_week_revoke_order_num
coalesce(sum(case when last_time between date_sub(next_day('${current_date}','MO'),14) and date_sub(next_day('${current_date}','MO'),8) then order_num end),0) as last_week_revoke_order_num,
--last_last_week_revoke_order_num
coalesce(sum(case when last_time between date_sub(next_day('${current_date}','MO'),21) and date_sub(next_day('${current_date}','MO'),15) then order_num end),0) as last_last_week_revoke_order_num
from
(--select online_driver by week
select driver_no,driver_name,driver_phone,lease_name,second_lease_name,lease_id,second_lease_no,car_service_type
from Operable_driver_online_time_simple where last_week_online_time is not null
) as last_week_online_driver
left join  --join online_driver and all_revoke_order for order_num
(select driver_no,order_num,last_time from driver_revoke_order_num)
on last_week_online_driver.driver_no=driver_revoke_order_num.driver_no
group by last_week_online_driver.driver_no,driver_name,driver_phone,lease_name,second_lease_name,lease_id,second_lease_no,car_service_type
)
where last_week_revoke_order_num>0
)
union all
(--select revoke_driver_m
select driver_no,driver_name,driver_phone,lease_name,second_lease_name,car_service_type,
last_month_revoke_order_num as last_cycle_revoke_order_num,last_last_month_revoke_order_num as last_last_cycle_revoke_order_num,3 as cycle_type,
lease_id,second_lease_no,regexp_replace(date_sub('${current_date}',1),'-','') as dt
from
(
select last_month_online_driver.driver_no,driver_name,driver_phone,lease_name,second_lease_name,lease_id,second_lease_no,car_service_type,
--last_month_revoke_order_num
coalesce(sum(case when last_time between trunc(add_months('${current_date}',-1),'MM') and date_sub(trunc('${current_date}','MM'),1) then order_num end),0) as last_month_revoke_order_num,
--last_last_month_revoke_order_num
coalesce(sum(case when last_time between trunc(add_months('${current_date}',-2),'MM') and date_sub(trunc(trunc(add_months('${current_date}',-1),'MM'),'MM'),1) then order_num end),0) as last_last_month_revoke_order_num
from
(--select online_driver by month
select driver_no,driver_name,driver_phone,lease_name,second_lease_name,lease_id,second_lease_no,car_service_type
from Operable_driver_online_time_simple where last_month_online_time is not null
) as last_month_online_driver
left join  --join online_driver and all_revoke_order for order_num
(select driver_no,order_num,last_time from driver_revoke_order_num)
on last_month_online_driver.driver_no=driver_revoke_order_num.driver_no
group by last_month_online_driver.driver_no,driver_name,driver_phone,lease_name,second_lease_name,lease_id,second_lease_no,car_service_type
)
where last_month_revoke_order_num>0
)

DISTRIBUTE BY dt;