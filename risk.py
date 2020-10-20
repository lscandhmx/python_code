/*
目的：众悦看板--风控
频次：每日
时间：2020-08-12
分区：动态分区
*/

/*设置动态分区*/
SET hive.exec.dynamic.partition=true; --使用动态分区
Set hive.exec.dynamic.partition.mode=nonstrict;--无限制模式
SET hive.exec.max.dynamic.partitions.pernode = 1000;--每个节点生成动态分区最大个数
SET hive.exec.max.dynamic.partitions=1000;--一个任务最多可以创建的文件数目


--risk_orde include day/week/month
with driver_Wind_control_order as
(
select risk_order.driver_no,driver_name,driver_phone,lease_name,second_lease_name,lease_id,second_lease_no,car_service_type,order_no,day_key from
(select driver_no, order_no,substr(use_time,1,10) as day_key,1 as ro from cc_dw.t_dwd__penalty__penalty_order__f where dt=regexp_replace(date_sub('${current_date}',1),'-','')
and is_appeal in(3,5)
and substr(use_time,1,10) between trunc(add_months('${current_date}',-2),'MM') and date_sub('${current_date}',1)
--union all--guashichang
--select if(get_json_object(driver_no,'$.$numberlong') is NOT NULL ,get_json_object(driver_no,'$.$numberlong') , driver_no) as driver_no,
--regexp_extract(order_no,'([0-9]+)',1) order_no,dt as day_key,row_number() over(partition by _id order by dt) ro
--from cc_ods.risk_mongo__earthshake__driver_risk_info having ro=1 and order_no is not null
--and concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) between trunc(add_months('${current_date}',-2),'MM') and date_sub('${current_date}',1)
) as risk_order
left join
(select driver_no as driver_no_1,driver_name,driver_phone,lease_name,second_lease_name,lease_id,second_lease_no,coalesce(car_service_type,0) as car_service_type from cc_dw.t_dwd_pub_cp_driver_job_info_1d
where dt=regexp_replace(date_sub('${current_date}',1),'-',''))as driver
on risk_order.driver_no=driver.driver_no_1
where driver_no_1 is not null
)

insert overwrite table cc_ads.zhongyue_abnormal_board_risk_order_driver_1d partition(dt)

---------------------unoin all---------------------------
--select Wind_control_order_driver_d
select driver_no,driver_name,driver_phone,lease_name,second_lease_name,car_service_type,
coalesce(count(case when day_key=date_sub('${current_date}',1) then order_no end),0) as last_cycle_Wind_control_order_num,
--before_yesterday_Wind_control_order_num
coalesce(count(case when day_key=date_sub('${current_date}',2) then order_no end),0) as last_last_cycle_Wind_control_order_num,
concat_ws(',',collect_set(case when day_key=date_sub('${current_date}',1) then order_no end)) as order_nos,
1 as cycle_type,lease_id,second_lease_no,regexp_replace(date_sub('${current_date}',1),'-','') as dt
from driver_Wind_control_order
group by driver_no,driver_name,driver_phone,lease_name,second_lease_name,lease_id,second_lease_no,car_service_type
having last_cycle_Wind_control_order_num>0
union all
--select Wind_control_order_driver_w
select driver_no,driver_name,driver_phone,lease_name,second_lease_name,car_service_type,
--last_week_Wind_control_order_num
coalesce(count(case when day_key between date_sub(next_day('${current_date}','MO'),14) and date_sub(next_day('${current_date}','MO'),8) then order_no end),0)
as last_cycle_Wind_control_order_num,
--last_last_week_Wind_control_order_num
coalesce(count(case when day_key between date_sub(next_day('${current_date}','MO'),21) and date_sub(next_day('${current_date}','MO'),15) then order_no end),0)
as last_last_cycle_Wind_control_order_num,
concat_ws(',',collect_set(case when day_key between date_sub(next_day('${current_date}','MO'),14) and date_sub(next_day('${current_date}','MO'),8) then order_no end)) as order_nos,
2 as cycle_type,lease_id,second_lease_no,regexp_replace(date_sub('${current_date}',1),'-','') as dt
from driver_Wind_control_order
group by driver_no,driver_name,driver_phone,lease_name,second_lease_name,lease_id,second_lease_no,car_service_type
having last_cycle_Wind_control_order_num>0
union all
--select Wind_control_order_driver_m
select driver_no,driver_name,driver_phone,lease_name,second_lease_name,car_service_type,
--last_month_Wind_control_order_num
coalesce(count(case when day_key between trunc(add_months('${current_date}',-1),'MM') and date_sub(trunc('${current_date}','MM'),1) then order_no end),0)
as last_cycle_Wind_control_order_num,
--last_last_month_Wind_control_order_num
coalesce(count(case when day_key between trunc(add_months('${current_date}',-2),'MM') and date_sub(trunc(trunc(add_months('${current_date}',-1),'MM'),'MM'),1) then order_no end),0)
as last_last_cycle_Wind_control_order_num,
concat_ws(',',collect_set(case when day_key between trunc(add_months('${current_date}',-1),'MM') and date_sub(trunc('${current_date}','MM'),1) then order_no end)) as order_nos,
3 as cycle_type,lease_id,second_lease_no,regexp_replace(date_sub('${current_date}',1),'-','') as dt
from driver_Wind_control_order
group by driver_no,driver_name,driver_phone,lease_name,second_lease_name,lease_id,second_lease_no,car_service_type
having last_cycle_Wind_control_order_num>0
DISTRIBUTE BY dt;