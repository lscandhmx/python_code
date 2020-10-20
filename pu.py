# -*- coding:utf-8 -*-

# -*- coding: utf8 -*-
import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, date, timedelta
from os import sys, path
import os.path
from airflow.operators.python_operator import PythonOperator
import pandas as pd
from datetime import datetime, timedelta, date
import MySQLdb

reload(sys)
sys.setdefaultencoding('utf-8')
sys.path.append(path.dirname(path.abspath(__file__)))
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
sys.path.append(path.dirname(path.dirname(path.dirname(path.abspath(__file__)))))
sys.path.append(path.dirname(path.dirname(path.dirname(path.dirname(path.abspath(__file__))))))
from utils import send_email, basic_util, env_conf
from airflow.utils.trigger_rule import TriggerRule
from airflow_plugins import DagDepCondition, CrossDagDepPythonOperator, HiveToMysqlByLoadFileOperator
from python.utils.mysql_conf import MysqlConfig
operation_center = MysqlConfig.get_db_conf("operation_center")

dag_id = basic_util.get_dag_id(path.abspath(__file__))
src_scripts_ads_parent_dir = '/home/admin/program/warehouse/sql/cc_ads/'
task_local_output_dir = env_conf.task_local_output_root_dir + "/bi/order/bi_qiankun_driver_zhongyue_dashboard/"

## Define the DAG object
default_args = {
    'owner': 'Shuangshuang.Weng',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['Shuangshuang.Weng@geely.com'],
    'email_on_failure': True
}
dag = DAG(dag_id, default_args=default_args, schedule_interval='34 8 * * *', concurrency=4)



# 开始的根节点任务，无实际内容
t_bi_start = DummyOperator(
    task_id='bi_start',
    default_args=default_args,
    dag=dag,
)

# 结束的尾节点任务，无实际内容
t_bi_end = DummyOperator(
    task_id='bi_end',
    default_args=default_args,
    dag=dag,
)


###未上线司机
def bi_qiankun_driver_offline_1d(*op_args, **context):
    exit_code = os.system(
        "spark-sql --master yarn --name bi_qiankun_driver_offline_1d --executor-memory 7G --num-executors 5 --executor-cores 5 --driver-memory 6G -f " + src_scripts_ads_parent_dir + "bi_qiankun_driver_online_board_1d.sql " + '--hiveconf current_date=' +
        context['tomorrow_ds'])
    if (exit_code != 0):
        raise Exception("执行spark-sql任务[xxx]失败,exit_code:{}".format(exit_code))


bi_qiankun_driver_offline_1d = CrossDagDepPythonOperator(
    task_id='bi_qiankun_driver_offline_1d',
    python_callable=bi_qiankun_driver_offline_1d,
    provide_context=True,
    dag_dep_conditions=[
        DagDepCondition(dag_id='cc_dw.dw_all', execution_date="%s" % (date.today() - timedelta(days=1)),
                        task_ids=['t_fct_driver_online','t_dwd_pub_cp_driver_job_info_1d'])
    ],
    dag=dag)


###未成单司机
def bi_qiankun_driver_unsuc_1d(*op_args, **context):
    exit_code = os.system(
        "spark-sql --master yarn --name bi_qiankun_driver_unsuc_1d --executor-memory 7G --num-executors 5 --executor-cores 5 --driver-memory 6G -f " + src_scripts_ads_parent_dir + "bi_qiankun_driver_unsuc_board_1d.sql " + '--hiveconf current_date=' +
        context['tomorrow_ds'])
    if (exit_code != 0):
        raise Exception("执行spark-sql任务[xxx]失败,exit_code:{}".format(exit_code))


bi_qiankun_driver_unsuc_1d = CrossDagDepPythonOperator(
    task_id='bi_qiankun_driver_unsuc_1d',
    python_callable=bi_qiankun_driver_unsuc_1d,
    provide_context=True,
    dag_dep_conditions=[
        DagDepCondition(dag_id='cc_dw.dw_all', execution_date="%s" % (date.today() - timedelta(days=1)),
                        task_ids=['t_dwd_boc_zhongyue_use_time_1d']),
        DagDepCondition(dag_id='cc_dw.cdm', execution_date="%s" % (date.today() - timedelta(days=1)),
                        task_ids=['t_dwd_pub_order_univ_use_time_1d']),
        DagDepCondition(dag_id='cc_dw.dw_all', execution_date="%s" % (date.today() - timedelta(days=1)),
                        task_ids=['t_fct_driver_online','t_dwd_pub_cp_driver_job_info_1d'])
    ],
    dag=dag)

###成单异常司机
def bi_qiankun_driver_suc_abnormal_1d(*op_args, **context):
    exit_code = os.system(
        "spark-sql --master yarn --name bi_qiankun_driver_suc_abnormal_1d --executor-memory 7G --num-executors 5 --executor-cores 5 --driver-memory 6G -f " + src_scripts_ads_parent_dir + "bi_qiankun_driver_suc_abnormal_board_1d.sql " + '--hiveconf current_date=' +
        context['tomorrow_ds'])
    if (exit_code != 0):
        raise Exception("执行spark-sql任务[xxx]失败,exit_code:{}".format(exit_code))


bi_qiankun_driver_suc_abnormal_1d = CrossDagDepPythonOperator(
    task_id='bi_qiankun_driver_suc_abnormal_1d',
    python_callable=bi_qiankun_driver_suc_abnormal_1d,
    provide_context=True,
    dag_dep_conditions=[
        DagDepCondition(dag_id='cc_dw.dw_all', execution_date="%s" % (date.today() - timedelta(days=1)),
                        task_ids=['t_dwd_boc_zhongyue_use_time_1d']),
        DagDepCondition(dag_id='cc_dw.cdm', execution_date="%s" % (date.today() - timedelta(days=1)),
                        task_ids=['t_dwd_pub_order_univ_use_time_1d']),
        DagDepCondition(dag_id='cc_dw.dw_all', execution_date="%s" % (date.today() - timedelta(days=1)),
                        task_ids=['t_fct_driver_online','t_dwd_pub_cp_driver_job_info_1d'])
    ],
    dag=dag)

###风控
def bi_qiankun_driver_risk_1d(*op_args, **context):
    exit_code = os.system(
        "spark-sql --master yarn --name bi_qiankun_driver_risk_1d --executor-memory 7G --num-executors 5 --executor-cores 5 --driver-memory 6G -f " + src_scripts_ads_parent_dir + "bi_qiankun_driver_risk_board_1d.sql " + '--hiveconf current_date=' +
        context['tomorrow_ds'])
    if (exit_code != 0):
        raise Exception("执行spark-sql任务[xxx]失败,exit_code:{}".format(exit_code))


bi_qiankun_driver_risk_1d = CrossDagDepPythonOperator(
    task_id='bi_qiankun_driver_risk_1d',
    python_callable=bi_qiankun_driver_risk_1d,
    provide_context=True,
    dag_dep_conditions=[
        DagDepCondition(dag_id='cc_dw.dwdnew', execution_date="%s" % (date.today() - timedelta(days=1)),
                        task_ids=['cc_dw.t_dwd__penalty__penalty_order__f']),
        DagDepCondition(dag_id='cc_dw.dw_all', execution_date="%s" % (date.today() - timedelta(days=1)),
                        task_ids=['t_fct_driver_online','t_dwd_pub_cp_driver_job_info_1d'])
    ],
    dag=dag)



###司机有责取消
def bi_qiankun_driver_cancel_1d(*op_args, **context):
    exit_code = os.system(
        "spark-sql --master yarn --name bi_qiankun_driver_cancel_1d --executor-memory 7G --num-executors 5 --executor-cores 5 --driver-memory 6G -f " + src_scripts_ads_parent_dir + "bi_qiankun_driver_cancel_board_1d.sql " + '--hiveconf current_date=' +
        context['tomorrow_ds'])
    if (exit_code != 0):
        raise Exception("执行spark-sql任务[xxx]失败,exit_code:{}".format(exit_code))


bi_qiankun_driver_cancel_1d = CrossDagDepPythonOperator(
    task_id='bi_qiankun_driver_cancel_1d',
    python_callable=bi_qiankun_driver_cancel_1d,
    provide_context=True,
    dag_dep_conditions=[
        DagDepCondition(dag_id='cc_dw.dw_all', execution_date="%s" % (date.today() - timedelta(days=1)),
                        task_ids=['t_dwd_boc_zhongyue_use_time_1d']),
        DagDepCondition(dag_id='cc_dw.cdm', execution_date="%s" % (date.today() - timedelta(days=1)),
                        task_ids=['t_dwd_pub_order_univ_use_time_1d']),
        DagDepCondition(dag_id='cc_dw.dw_all', execution_date="%s" % (date.today() - timedelta(days=1)),
                        task_ids=['t_fct_driver_online','t_dwd_pub_cp_driver_job_info_1d'])
    ],
    dag=dag)




###司机客诉
def bi_qiankun_driver_complaint_1d(*op_args, **context):
    exit_code = os.system(
        "spark-sql --master yarn --name bi_qiankun_driver_complaint_1d --executor-memory 7G --num-executors 5 --executor-cores 5 --driver-memory 6G -f " + src_scripts_ads_parent_dir + "bi_qiankun_driver_complaint_board_1d.sql " + '--hiveconf current_date=' +
        context['tomorrow_ds'])
    if (exit_code != 0):
        raise Exception("执行spark-sql任务[xxx]失败,exit_code:{}".format(exit_code))


bi_qiankun_driver_complaint_1d = CrossDagDepPythonOperator(
    task_id='bi_qiankun_driver_complaint_1d',
    python_callable=bi_qiankun_driver_complaint_1d,
    provide_context=True,
    dag_dep_conditions=[
        DagDepCondition(dag_id='cc_dw.dwdnew', execution_date="%s" % (date.today() - timedelta(days=1)),
                        task_ids=['cc_dw.t_dwd__service_center__csc_work_order__f','cc_dw.t_dwd__service_center__csc_work_order_detail__f','cc_dw.t_dwd__service_center__csc_work_order_classify__f']),
        DagDepCondition(dag_id='cc_dw.dw_all', execution_date="%s" % (date.today() - timedelta(days=1)),
                        task_ids=['t_fct_driver_online','t_dwd_pub_cp_driver_job_info_1d']),
        DagDepCondition(dag_id='cc_dw.dim', execution_date="%s" % (date.today() - timedelta(days=1)),
                        task_ids=['t_dim_csr_work_order_classify_1df'])
    ],
    dag=dag)



###统计表
def bi_qiankun_driver_statistics_1d(*op_args, **context):
    exit_code = os.system(
        "spark-sql --master yarn --name bi_qiankun_driver_statistics_1d --executor-memory 7G --num-executors 5 --executor-cores 5 --driver-memory 6G -f " + src_scripts_ads_parent_dir + "bi_qiankun_driver_statistics_board_1d.sql " + '--hiveconf current_date=' +
        context['tomorrow_ds'])
    if (exit_code != 0):
        raise Exception("执行spark-sql任务[xxx]失败,exit_code:{}".format(exit_code))


bi_qiankun_driver_statistics_1d = CrossDagDepPythonOperator(
    task_id='bi_qiankun_driver_statistics_1d',
    python_callable=bi_qiankun_driver_statistics_1d,
    provide_context=True,
    dag_dep_conditions=[
        DagDepCondition(dag_id='cc_dw.dwdnew', execution_date="%s" % (date.today() - timedelta(days=1)),
                        task_ids=['cc_dw.t_dwd__penalty__penalty_order__f']),
        DagDepCondition(dag_id='cc_dw.dw_all', execution_date="%s" % (date.today() - timedelta(days=1)),
                        task_ids=['t_fct_driver_online', 't_dwd_pub_driver_personas_1d'])
    ],
    dag=dag)


###################################################################################################

def bi_qiankun_driver_offline(*op_args, **context):

    # 确保输出目录存在
    if not path.exists(task_local_output_dir):
        os.makedirs(task_local_output_dir, 0755)

    yesterday_dt = (datetime.today() - timedelta(days=1)).strftime('%Y%m%d')


    sql = '''
          select driver_no,driver_name,driver_phone,lease_name,second_lease_name,car_service_type,last_cycle_online_time,last_last_cycle_online_time,cycle_type,lease_id,second_lease_no
          from cc_ads.zhongyue_abnormal_board_offline_driver_1d2
          where dt ='{execution_dt}'  '''.format(execution_dt=context['ds_nodash'])

    print "sql:" + sql

    task_output_file1 = task_local_output_dir + "/bi_qiankun_driver_offline.log.{yesterday_dt}".format(
        yesterday_dt=yesterday_dt)
    exit_code = os.system("spark-sql --executor-memory 4G --num-executors 4 --executor-cores 2 --driver-memory 3G  -e\"" + sql + "\"  > {task_output_file} ".format(task_output_file=task_output_file1))
    print("bi_qiankun_driver_offline,exit_code:", exit_code)
    if (exit_code != 0):
        raise Exception("执行spark-sql任务处理bi_qiankun_driver_offline失败")
    print ("spark-sql执行bi_qiankun_driver_offline成功")

    # 数据重跑-清除数据
    mysql_command = "mysql -P{mysql_port} -u {mysql_user} -p{mysql_password} -h {mysql_host} " \
                    "-e \"delete from operation_center.zy_abnormal_board_offline_driver_1d   \" ".format(
        # mysql_port=env_conf.report_center_mysql_port,
        # mysql_host=env_conf.report_center_mysql_host,
        # mysql_user=env_conf.report_center_mysql_user,
        # mysql_password=env_conf.report_center_mysql_password,
        execution_dt=context['ds_nodash'],
        # mysql_port='15381',
        # mysql_host='10.30.200.155',
        # mysql_user='testAdmin',
        # mysql_password='adminTest'
        mysql_port = '15381',
        mysql_host = '172.16.100.68',
        mysql_user = 'operation_center',
        mysql_password = 'kL4Y5lU4LZ9phwdj'
    )
    print("开始清除执行当天的实时数据...，命令：\n{command}".format(command=mysql_command))

    exit_code = os.system(mysql_command)
    if (exit_code != 0):
        raise Exception("执行删除数据失败,exit_code={exit_code}".format(exit_code=exit_code))



    mysql_command = "sed -i 's/NULL/\\\\N/g' {task_output_file} && "\
                    "mysql -P{mysql_port} -u {mysql_user} -p{mysql_password} -h {mysql_host} " \
                    "-e \"load data LOCAL infile '{task_output_file}' " \
                    "replace into table operation_center.zy_abnormal_board_offline_driver_1d fields terminated by '\\t' (driver_no,driver_name,driver_phone,lease_name,second_lease_name,car_service_type,last_cycle_online_time,last_last_cycle_online_time,cycle_type,lease_id,second_lease_no)\"".format(

        mysql_port='15381',
        mysql_host='172.16.100.68',
        mysql_user='operation_center',
        mysql_password='kL4Y5lU4LZ9phwdj',
        task_output_file=task_output_file1

       # mysql_port='15381',
       # mysql_host='172.16.100.68',
       # mysql_user='operation_center',
       # mysql_password='kL4Y5lU4LZ9phwdj',
       # task_output_file=task_output_file1
    )
    print ("开始load数据到operation_center库...，命令：\n{command}".format(command=mysql_command))
    exit_code = os.system(mysql_command)
    if (exit_code != 0):
        raise Exception("执行load数据到operation_center库失败,exit_code={exit_code}".format(exit_code=exit_code))

    print ("Done")


bi_qiankun_driver_offline_hive2mysql = PythonOperator(
    task_id='bi_qiankun_driver_offline_hive2mysql',
    python_callable=bi_qiankun_driver_offline,
    provide_context=True,
    dag=dag)


#####未成单
def bi_qiankun_driver_unsuc(*op_args, **context):

    # 确保输出目录存在
    if not path.exists(task_local_output_dir):
        os.makedirs(task_local_output_dir, 0755)

    yesterday_dt = (datetime.today() - timedelta(days=1)).strftime('%Y%m%d')


    sql = '''
          select driver_no,driver_name,driver_phone,lease_name,second_lease_name,car_service_type,last_cycle_finish_order_num,last_last_cycle_finish_order_num,cycle_type,lease_id,second_lease_no
          from cc_ads.zhongyue_abnormal_board_unfinished_order_driver_1d
          where dt ='{execution_dt}'  '''.format(execution_dt=context['ds_nodash'])

    print "sql:" + sql

    task_output_file1 = task_local_output_dir + "/bi_qiankun_driver_unsuc.log.{yesterday_dt}".format(
        yesterday_dt=yesterday_dt)
    exit_code = os.system("spark-sql --executor-memory 4G --num-executors 4 --executor-cores 2 --driver-memory 3G  -e\"" + sql + "\"  > {task_output_file} ".format(task_output_file=task_output_file1))
    print("bi_qiankun_driver_unsuc,exit_code:", exit_code)
    if (exit_code != 0):
        raise Exception("执行spark-sql任务处理bi_qiankun_driver_unsuc失败")
    print ("spark-sql执行bi_qiankun_driver_unsuc成功")

    # 数据重跑-清除数据
    mysql_command = "mysql -P{mysql_port} -u {mysql_user} -p{mysql_password} -h {mysql_host} " \
                    "-e \"delete from operation_center.zy_abnormal_board_unsuc_driver_1d   \" ".format(
        # mysql_port=env_conf.report_center_mysql_port,
        # mysql_host=env_conf.report_center_mysql_host,
        # mysql_user=env_conf.report_center_mysql_user,
        # mysql_password=env_conf.report_center_mysql_password,
        execution_dt=context['ds_nodash'],
        mysql_port='15381',
        mysql_host='172.16.100.68',
        mysql_user='operation_center',
        mysql_password='kL4Y5lU4LZ9phwdj'
    )
    print("开始清除执行当天的实时数据...，命令：\n{command}".format(command=mysql_command))

    exit_code = os.system(mysql_command)
    if (exit_code != 0):
        raise Exception("执行删除数据失败,exit_code={exit_code}".format(exit_code=exit_code))



    mysql_command = "sed -i 's/NULL/\\\\N/g' {task_output_file} && "\
                    "mysql -P{mysql_port} -u {mysql_user} -p{mysql_password} -h {mysql_host} " \
                    "-e \"load data LOCAL infile '{task_output_file}' " \
                    "replace into table operation_center.zy_abnormal_board_unsuc_driver_1d fields terminated by '\\t' (driver_no,driver_name,driver_phone,lease_name,second_lease_name,car_service_type,last_cycle_finish_order_num,last_last_cycle_finish_order_num,cycle_type,lease_id,second_lease_no)\"".format(

        mysql_port='15381',
        mysql_host='172.16.100.68',
        mysql_user='operation_center',
        mysql_password='kL4Y5lU4LZ9phwdj',
        task_output_file=task_output_file1

       # mysql_port='15381',
       # mysql_host='172.16.100.68',
       # mysql_user='operation_center',
       # mysql_password='kL4Y5lU4LZ9phwdj',
       # task_output_file=task_output_file1
    )
    print ("开始load数据到operation_center库...，命令：\n{command}".format(command=mysql_command))
    exit_code = os.system(mysql_command)
    if (exit_code != 0):
        raise Exception("执行load数据到operation_center库失败,exit_code={exit_code}".format(exit_code=exit_code))

    print ("Done")


bi_qiankun_driver_unsuc_hive2mysql = PythonOperator(
    task_id='bi_qiankun_driver_unsuc_hive2mysql',
    python_callable=bi_qiankun_driver_unsuc,
    provide_context=True,
    dag=dag)


#####成单异常
def bi_qiankun_driver_suc_abnormal(*op_args, **context):

    # 确保输出目录存在
    if not path.exists(task_local_output_dir):
        os.makedirs(task_local_output_dir, 0755)

    yesterday_dt = (datetime.today() - timedelta(days=1)).strftime('%Y%m%d')


    sql = '''
          select driver_no,driver_name,driver_phone,lease_name,second_lease_name,car_service_type,last_cycle_finish_order_num,last_last_cycle_finish_order_num,abnormality,cycle_type,lease_id,second_lease_no
          from cc_ads.zhongyue_abnormal_board_order_abnormal_driver_1d
          where dt ='{execution_dt}'  '''.format(execution_dt=context['ds_nodash'])

    print "sql:" + sql

    task_output_file1 = task_local_output_dir + "/bi_qiankun_driver_suc_abnormal.log.{yesterday_dt}".format(
        yesterday_dt=yesterday_dt)
    exit_code = os.system("spark-sql --executor-memory 4G --num-executors 4 --executor-cores 2 --driver-memory 3G  -e\"" + sql + "\"  > {task_output_file} ".format(task_output_file=task_output_file1))
    print("bi_qiankun_driver_suc_abnormal,exit_code:", exit_code)
    if (exit_code != 0):
        raise Exception("执行spark-sql任务处理bi_qiankun_driver_suc_abnormal失败")
    print ("spark-sql执行bi_qiankun_driver_suc_abnormal成功")

    # 数据重跑-清除数据
    mysql_command = "mysql -P{mysql_port} -u {mysql_user} -p{mysql_password} -h {mysql_host} " \
                    "-e \"delete from operation_center.zy_abnormal_board_order_abnormal_driver_1d   \" ".format(
        # mysql_port=env_conf.report_center_mysql_port,
        # mysql_host=env_conf.report_center_mysql_host,
        # mysql_user=env_conf.report_center_mysql_user,
        # mysql_password=env_conf.report_center_mysql_password,
        execution_dt=context['ds_nodash'],
        mysql_port='15381',
        mysql_host='172.16.100.68',
        mysql_user='operation_center',
        mysql_password='kL4Y5lU4LZ9phwdj'
    )
    print("开始清除执行当天的实时数据...，命令：\n{command}".format(command=mysql_command))

    exit_code = os.system(mysql_command)
    if (exit_code != 0):
        raise Exception("执行删除数据失败,exit_code={exit_code}".format(exit_code=exit_code))



    mysql_command = "sed -i 's/NULL/\\\\N/g' {task_output_file} && "\
                    "mysql -P{mysql_port} -u {mysql_user} -p{mysql_password} -h {mysql_host} " \
                    "-e \"load data LOCAL infile '{task_output_file}' " \
                    "replace into table operation_center.zy_abnormal_board_order_abnormal_driver_1d fields terminated by '\\t' (driver_no,driver_name,driver_phone,lease_name,second_lease_name,car_service_type,last_cycle_finish_order_num,last_last_cycle_finish_order_num,abnormality,cycle_type,lease_id,second_lease_no)\"".format(

        mysql_port='15381',
        mysql_host='172.16.100.68',
        mysql_user='operation_center',
        mysql_password='kL4Y5lU4LZ9phwdj',
        task_output_file=task_output_file1

       # mysql_port='15381',
       # mysql_host='172.16.100.68',
       # mysql_user='operation_center',
       # mysql_password='kL4Y5lU4LZ9phwdj',
       # task_output_file=task_output_file1
    )
    print ("开始load数据到operation_center库...，命令：\n{command}".format(command=mysql_command))
    exit_code = os.system(mysql_command)
    if (exit_code != 0):
        raise Exception("执行load数据到operation_center库失败,exit_code={exit_code}".format(exit_code=exit_code))

    print ("Done")


bi_qiankun_driver_suc_abnormal2mysql = PythonOperator(
    task_id='bi_qiankun_driver_suc_abnormal2mysql',
    python_callable=bi_qiankun_driver_suc_abnormal,
    provide_context=True,
    dag=dag)


#####风控
def bi_qiankun_driver_risk(*op_args, **context):

    # 确保输出目录存在
    if not path.exists(task_local_output_dir):
        os.makedirs(task_local_output_dir, 0755)

    yesterday_dt = (datetime.today() - timedelta(days=1)).strftime('%Y%m%d')


    sql = '''
          select driver_no,driver_name,driver_phone,lease_name,second_lease_name,car_service_type,last_cycle_wind_control_order_num,last_last_cycle_wind_control_order_num,order_nos,cycle_type,lease_id,second_lease_no
          from cc_ads.zhongyue_abnormal_board_risk_order_driver_1d
          where dt ='{execution_dt}'  '''.format(execution_dt=context['ds_nodash'])

    print "sql:" + sql

    task_output_file1 = task_local_output_dir + "/bi_qiankun_driver_risk.log.{yesterday_dt}".format(
        yesterday_dt=yesterday_dt)
    exit_code = os.system("spark-sql --executor-memory 4G --num-executors 4 --executor-cores 2 --driver-memory 3G  -e\"" + sql + "\"  > {task_output_file} ".format(task_output_file=task_output_file1))
    print("bi_qiankun_driver_risk,exit_code:", exit_code)
    if (exit_code != 0):
        raise Exception("执行spark-sql任务处理bi_qiankun_driver_risk失败")
    print ("spark-sql执行bi_qiankun_driver_risk成功")

    # 数据重跑-清除数据
    mysql_command = "mysql -P{mysql_port} -u {mysql_user} -p{mysql_password} -h {mysql_host} " \
                    "-e \"delete from operation_center.zy_abnormal_board_risk_order_1d   \" ".format(
        # mysql_port=env_conf.report_center_mysql_port,
        # mysql_host=env_conf.report_center_mysql_host,
        # mysql_user=env_conf.report_center_mysql_user,
        # mysql_password=env_conf.report_center_mysql_password,
        execution_dt=context['ds_nodash'],
        mysql_port='15381',
        mysql_host='172.16.100.68',
        mysql_user='operation_center',
        mysql_password='kL4Y5lU4LZ9phwdj'
    )
    print("开始清除执行当天的实时数据...，命令：\n{command}".format(command=mysql_command))

    exit_code = os.system(mysql_command)
    if (exit_code != 0):
        raise Exception("执行删除数据失败,exit_code={exit_code}".format(exit_code=exit_code))



    mysql_command = "sed -i 's/NULL/\\\\N/g' {task_output_file} && "\
                    "mysql -P{mysql_port} -u {mysql_user} -p{mysql_password} -h {mysql_host} " \
                    "-e \"load data LOCAL infile '{task_output_file}' " \
                    "replace into table operation_center.zy_abnormal_board_risk_order_1d fields terminated by '\\t' (driver_no,driver_name,driver_phone,lease_name,second_lease_name,car_service_type,last_cycle_wind_control_order_num,last_last_cycle_wind_control_order_num,order_nos,cycle_type,lease_id,second_lease_no)\"".format(

        mysql_port='15381',
        mysql_host='172.16.100.68',
        mysql_user='operation_center',
        mysql_password='kL4Y5lU4LZ9phwdj',
        task_output_file=task_output_file1

       # mysql_port='15381',
       # mysql_host='172.16.100.68',
       # mysql_user='operation_center',
       # mysql_password='kL4Y5lU4LZ9phwdj',
       # task_output_file=task_output_file1
    )
    print ("开始load数据到operation_center库...，命令：\n{command}".format(command=mysql_command))
    exit_code = os.system(mysql_command)
    if (exit_code != 0):
        raise Exception("执行load数据到operation_center库失败,exit_code={exit_code}".format(exit_code=exit_code))

    print ("Done")


bi_qiankun_driver_risk2mysql = PythonOperator(
    task_id='bi_qiankun_driver_risk2mysql',
    python_callable=bi_qiankun_driver_risk,
    provide_context=True,
    dag=dag)

####客诉
def bi_qiankun_driver_complaint(*op_args, **context):

    # 确保输出目录存在
    if not path.exists(task_local_output_dir):
        os.makedirs(task_local_output_dir, 0755)

    yesterday_dt = (datetime.today() - timedelta(days=1)).strftime('%Y%m%d')


    sql = '''
          select driver_no,driver_name,driver_phone,lease_name,second_lease_name,car_service_type,order_no,classify_name,complaint_describe,cycle_type,lease_id,second_lease_no
          from cc_ads.Zhongyue_abnormal_board_complaint_driver_1d
          where dt ='{execution_dt}'  '''.format(execution_dt=context['ds_nodash'])

    print "sql:" + sql

    task_output_file1 = task_local_output_dir + "/bi_qiankun_driver_complaint.log.{yesterday_dt}".format(
        yesterday_dt=yesterday_dt)
    exit_code = os.system("spark-sql --executor-memory 4G --num-executors 4 --executor-cores 2 --driver-memory 3G  -e\"" + sql + "\"  > {task_output_file} ".format(task_output_file=task_output_file1))
    print("bi_qiankun_driver_complaint,exit_code:", exit_code)
    if (exit_code != 0):
        raise Exception("执行spark-sql任务处理bi_qiankun_driver_complaint失败")
    print ("spark-sql执行bi_qiankun_driver_complaint成功")

    # 数据重跑-清除数据
    mysql_command = "mysql -P{mysql_port} -u {mysql_user} -p{mysql_password} -h {mysql_host} " \
                    "-e \"delete from operation_center.zy_abnormal_board_complaint_1d   \" ".format(
        # mysql_port=env_conf.report_center_mysql_port,
        # mysql_host=env_conf.report_center_mysql_host,
        # mysql_user=env_conf.report_center_mysql_user,
        # mysql_password=env_conf.report_center_mysql_password,
        execution_dt=context['ds_nodash'],
        mysql_port='15381',
        mysql_host='172.16.100.68',
        mysql_user='operation_center',
        mysql_password='kL4Y5lU4LZ9phwdj'
    )
    print("开始清除执行当天的实时数据...，命令：\n{command}".format(command=mysql_command))

    exit_code = os.system(mysql_command)
    if (exit_code != 0):
        raise Exception("执行删除数据失败,exit_code={exit_code}".format(exit_code=exit_code))



    mysql_command = "sed -i 's/NULL/\\\\N/g' {task_output_file} && "\
                    "mysql -P{mysql_port} -u {mysql_user} -p{mysql_password} -h {mysql_host} " \
                    "-e \"load data LOCAL infile '{task_output_file}' " \
                    "replace into table operation_center.zy_abnormal_board_complaint_1d fields terminated by '\\t' (driver_no,driver_name,driver_phone,lease_name,second_lease_name,car_service_type,order_no,classify_name,complaint_describe,cycle_type,lease_id,second_lease_no)\"".format(

        mysql_port='15381',
        mysql_host='172.16.100.68',
        mysql_user='operation_center',
        mysql_password='kL4Y5lU4LZ9phwdj',
        task_output_file=task_output_file1

       # mysql_port='15381',
       # mysql_host='172.16.100.68',
       # mysql_user='operation_center',
       # mysql_password='kL4Y5lU4LZ9phwdj',
       # task_output_file=task_output_file1
    )
    print ("开始load数据到operation_center库...，命令：\n{command}".format(command=mysql_command))
    exit_code = os.system(mysql_command)
    if (exit_code != 0):
        raise Exception("执行load数据到operation_center库失败,exit_code={exit_code}".format(exit_code=exit_code))

    print ("Done")


bi_qiankun_driver_complaint_hive2mysql = PythonOperator(
    task_id='bi_qiankun_driver_complaint_hive2mysql',
    python_callable=bi_qiankun_driver_complaint,
    provide_context=True,
    dag=dag)


####有责取消
def bi_qiankun_driver_cancel(*op_args, **context):

    # 确保输出目录存在
    if not path.exists(task_local_output_dir):
        os.makedirs(task_local_output_dir, 0755)

    yesterday_dt = (datetime.today() - timedelta(days=1)).strftime('%Y%m%d')


    sql = '''
          select driver_no,driver_name,driver_phone,lease_name,second_lease_name,car_service_type,last_cycle_revoke_order_num,last_last_cycle_revoke_order_num,cycle_type,lease_id,second_lease_no
          from cc_ads.zhongyue_abnormal_board_cancel_order_driver_1d
          where dt ='{execution_dt}'  '''.format(execution_dt=context['ds_nodash'])

    print "sql:" + sql

    task_output_file1 = task_local_output_dir + "/bi_qiankun_driver_cancel.log.{yesterday_dt}".format(
        yesterday_dt=yesterday_dt)
    exit_code = os.system("spark-sql --executor-memory 4G --num-executors 4 --executor-cores 2 --driver-memory 3G  -e\"" + sql + "\"  > {task_output_file} ".format(task_output_file=task_output_file1))
    print("bi_qiankun_driver_cancel,exit_code:", exit_code)
    if (exit_code != 0):
        raise Exception("执行spark-sql任务处理bi_qiankun_driver_cancel失败")
    print ("spark-sql执行bi_qiankun_driver_cancel成功")

    # 数据重跑-清除数据
    mysql_command = "mysql -P{mysql_port} -u {mysql_user} -p{mysql_password} -h {mysql_host} " \
                    "-e \"delete from operation_center.zy_abnormal_board_cancel_order_1d   \" ".format(
        # mysql_port=env_conf.report_center_mysql_port,
        # mysql_host=env_conf.report_center_mysql_host,
        # mysql_user=env_conf.report_center_mysql_user,
        # mysql_password=env_conf.report_center_mysql_password,
        execution_dt=context['ds_nodash'],
        mysql_port='15381',
        mysql_host='172.16.100.68',
        mysql_user='operation_center',
        mysql_password='kL4Y5lU4LZ9phwdj'
    )
    print("开始清除执行当天的实时数据...，命令：\n{command}".format(command=mysql_command))

    exit_code = os.system(mysql_command)
    if (exit_code != 0):
        raise Exception("执行删除数据失败,exit_code={exit_code}".format(exit_code=exit_code))



    mysql_command = "sed -i 's/NULL/\\\\N/g' {task_output_file} && "\
                    "mysql -P{mysql_port} -u {mysql_user} -p{mysql_password} -h {mysql_host} " \
                    "-e \"load data LOCAL infile '{task_output_file}' " \
                    "replace into table operation_center.zy_abnormal_board_cancel_order_1d fields terminated by '\\t' (driver_no,driver_name,driver_phone,lease_name,second_lease_name,car_service_type,last_cycle_revoke_order_num,last_last_cycle_revoke_order_num,cycle_type,lease_id,second_lease_no)\"".format(

        mysql_port='15381',
        mysql_host='172.16.100.68',
        mysql_user='operation_center',
        mysql_password='kL4Y5lU4LZ9phwdj',
        task_output_file=task_output_file1

       # mysql_port='15381',
       # mysql_host='172.16.100.68',
       # mysql_user='operation_center',
       # mysql_password='kL4Y5lU4LZ9phwdj',
       # task_output_file=task_output_file1
    )
    print ("开始load数据到operation_center库...，命令：\n{command}".format(command=mysql_command))
    exit_code = os.system(mysql_command)
    if (exit_code != 0):
        raise Exception("执行load数据到operation_center库失败,exit_code={exit_code}".format(exit_code=exit_code))

    print ("Done")


bi_qiankun_driver_cancel_hive2mysql = PythonOperator(
    task_id='bi_qiankun_driver_cancel_hive2mysql',
    python_callable=bi_qiankun_driver_cancel,
    provide_context=True,
    dag=dag)

####统计表
def bi_qiankun_driver_statistics(*op_args, **context):

    # 确保输出目录存在
    if not path.exists(task_local_output_dir):
        os.makedirs(task_local_output_dir, 0755)

    yesterday_dt = (datetime.today() - timedelta(days=1)).strftime('%Y%m%d')


    sql = '''
          select lease_name,second_lease_name,car_service_type,driver_num,tabel_options,day_key,lease_id,second_lease_no
          from cc_ads.zhongyue_abnormal_board_all_driver_num_1d
          where dt ='{execution_dt}'  '''.format(execution_dt=context['ds_nodash'])

    print "sql:" + sql

    task_output_file1 = task_local_output_dir + "/bi_qiankun_driver_statistics.log.{yesterday_dt}".format(
        yesterday_dt=yesterday_dt)
    exit_code = os.system("spark-sql --executor-memory 4G --num-executors 4 --executor-cores 2 --driver-memory 3G  -e\"" + sql + "\"  > {task_output_file} ".format(task_output_file=task_output_file1))
    print("bi_qiankun_driver_statistics,exit_code:", exit_code)
    if (exit_code != 0):
        raise Exception("执行spark-sql任务处理bi_qiankun_driver_statistics失败")
    print ("spark-sql执行bi_qiankun_driver_statistics成功")


    mysql_command = "sed -i 's/NULL/\\\\N/g' {task_output_file} && "\
                    "mysql -P{mysql_port} -u {mysql_user} -p{mysql_password} -h {mysql_host} " \
                    "-e \"load data LOCAL infile '{task_output_file}' " \
                    "replace into table operation_center.zy_abnormal_board_statistics_1d2 fields terminated by '\\t' (lease_name,second_lease_name,car_service_type,driver_num,tabel_options,day_key,lease_id,second_lease_no)\"".format(

        mysql_port='15381',
        mysql_host='172.16.100.68',
        mysql_user='operation_center',
        mysql_password='kL4Y5lU4LZ9phwdj',
        task_output_file=task_output_file1

       # mysql_port='15381',
       # mysql_host='172.16.100.68',
       # mysql_user='operation_center',
       # mysql_password='kL4Y5lU4LZ9phwdj',
       # task_output_file=task_output_file1
    )
    print ("开始load数据到operation_center库...，命令：\n{command}".format(command=mysql_command))
    exit_code = os.system(mysql_command)
    if (exit_code != 0):
        raise Exception("执行load数据到operation_center库失败,exit_code={exit_code}".format(exit_code=exit_code))

    print ("Done")


bi_qiankun_driver_statistics_hive2mysql = PythonOperator(
    task_id='bi_qiankun_driver_statistics_hive2mysql',
    python_callable=bi_qiankun_driver_statistics,
    provide_context=True,
    dag=dag)


###################################################################################################

bi_qiankun_driver_offline_1d.set_upstream(t_bi_start)
bi_qiankun_driver_unsuc_1d.set_upstream(t_bi_start)
bi_qiankun_driver_suc_abnormal_1d.set_upstream(t_bi_start)
bi_qiankun_driver_risk_1d.set_upstream(t_bi_start)
bi_qiankun_driver_cancel_1d.set_upstream(t_bi_start)
bi_qiankun_driver_complaint_1d.set_upstream(t_bi_start)


bi_qiankun_driver_offline_hive2mysql.set_upstream(bi_qiankun_driver_offline_1d)
bi_qiankun_driver_unsuc_hive2mysql.set_upstream(bi_qiankun_driver_unsuc_1d)
bi_qiankun_driver_suc_abnormal2mysql.set_upstream(bi_qiankun_driver_suc_abnormal_1d)
bi_qiankun_driver_risk2mysql.set_upstream(bi_qiankun_driver_risk_1d)
bi_qiankun_driver_complaint_hive2mysql.set_upstream(bi_qiankun_driver_complaint_1d)
bi_qiankun_driver_cancel_hive2mysql.set_upstream(bi_qiankun_driver_cancel_1d)



bi_qiankun_driver_statistics_1d.set_upstream(bi_qiankun_driver_offline_1d)
bi_qiankun_driver_statistics_1d.set_upstream(bi_qiankun_driver_unsuc_1d)
bi_qiankun_driver_statistics_1d.set_upstream(bi_qiankun_driver_suc_abnormal_1d)
bi_qiankun_driver_statistics_1d.set_upstream(bi_qiankun_driver_cancel_1d)
bi_qiankun_driver_statistics_1d.set_upstream(bi_qiankun_driver_complaint_1d)

bi_qiankun_driver_statistics_hive2mysql.set_upstream(bi_qiankun_driver_statistics_1d)

bi_qiankun_driver_offline_hive2mysql.set_downstream(t_bi_end)
bi_qiankun_driver_unsuc_hive2mysql.set_downstream(t_bi_end)
bi_qiankun_driver_suc_abnormal2mysql.set_downstream(t_bi_end)
bi_qiankun_driver_risk2mysql.set_downstream(t_bi_end)
bi_qiankun_driver_complaint_hive2mysql.set_downstream(t_bi_end)
bi_qiankun_driver_cancel_hive2mysql.set_downstream(t_bi_end)

bi_qiankun_driver_statistics_hive2mysql.set_downstream(t_bi_end)
