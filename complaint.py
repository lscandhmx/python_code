/*
目的：众悦看板--客诉
频次：每日
时间：2020-08-12
分区：动态分区
*/

/*设置动态分区*/
SET hive.exec.dynamic.partition=true; --使用动态分区
Set hive.exec.dynamic.partition.mode=nonstrict;--无限制模式
SET hive.exec.max.dynamic.partitions.pernode = 1000;--每个节点生成动态分区最大个数
SET hive.exec.max.dynamic.partitions=1000;--一个任务最多可以创建的文件数目




--Complaint_orde include day/week/month
with Complaint_order_f as
(
select driver_no,driver_name,driver_phone,lease_name,second_lease_name,lease_id,second_lease_no,car_service_type,order_no,classify_name,complaint_describe,fjt
from
(SELECT distinct id,order_no,name3 as classify_name from
(select id,order_no,problem_classify from cc_dw.t_dwd__service_center__csc_work_order__f
where dt=regexp_replace(date_sub('${current_date}',1),'-','') and is_complaint_driver=1 and order_no!='0'
and substr(create_time,1,10) between trunc(add_months('${current_date}',-3),'MM') and date_sub('${current_date}',1)) as complaint_order1
left join
(select * from cc_dw.t_dim_csr_work_order_classify_1df
where dt=regexp_replace(date_sub('${current_date}',1),'-','')) as problem_classify
on (complaint_order1.problem_classify=problem_classify.id1 or
    complaint_order1.problem_classify=problem_classify.id2 or
    complaint_order1.problem_classify=problem_classify.id3 or
    complaint_order1.problem_classify=problem_classify.id4 or
    complaint_order1.problem_classify=problem_classify.id5)) as complaint_order
left join
(select work_order_id,substr(final_judgment_time,1,10) as fjt from cc_ods.mysql__service_center__csc_work_order_time_detail
where concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) between trunc(add_months('${current_date}',-3),'MM') and date_sub('${current_date}',1)
) as judgment
on complaint_order.id=judgment.work_order_id
left join
(select work_order_id,complaint_describe,is_driver_blame_second,driver_id as driver_no from cc_dw.t_dwd__service_center__csc_work_order_detail__f
where dt=regexp_replace(date_sub('${current_date}',1),'-','')  and driver_id>0
and substr(create_time,1,10) between trunc(add_months('${current_date}',-3),'MM') and date_sub('${current_date}',1)) as complaint_order_detail
on complaint_order.id=complaint_order_detail.work_order_id
left join
(select driver_no as driver_no_1,driver_name,driver_phone,lease_name,second_lease_name,lease_id,second_lease_no,coalesce(car_service_type,0) as car_service_type from cc_dw.t_dwd_pub_cp_driver_job_info_1d
where dt=regexp_replace(date_sub('${current_date}',1),'-',''))as driver
on complaint_order_detail.driver_no=driver.driver_no_1
where fjt is not null
and driver_no_1 is not null
and is_driver_blame_second !=1
)


insert overwrite table cc_ads.Zhongyue_abnormal_board_complaint_driver_1d partition(dt)

---------------------unoin all---------------------------
--select Complaint_order_driver_d
select driver_no,driver_name,driver_phone,lease_name,second_lease_name,car_service_type,order_no,classify_name,complaint_describe,1 as cycle_type,
lease_id,second_lease_no,regexp_replace(date_sub('${current_date}',1),'-','') as dt
from Complaint_order_f where fjt=date_sub('${current_date}',1)
union all
(--select Complaint_order_driver_w
select driver_no,driver_name,driver_phone,lease_name,second_lease_name,car_service_type,order_no,classify_name,complaint_describe,2 as cycle_type,
lease_id,second_lease_no,regexp_replace(date_sub('${current_date}',1),'-','') as dt
from Complaint_order_f where fjt between date_sub(next_day('${current_date}','MO'),14) and date_sub(next_day('${current_date}','MO'),8)
)
union all
(--select Complaint_order_driver_m
select driver_no,driver_name,driver_phone,lease_name,second_lease_name,car_service_type,order_no,classify_name,complaint_describe,3 as cycle_type,
lease_id,second_lease_no,regexp_replace(date_sub('${current_date}',1),'-','') as dt
from Complaint_order_f where fjt between trunc(add_months('${current_date}',-1),'MM') and date_sub(trunc('${current_date}','MM'),1))

DISTRIBUTE BY dt;