#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# date:   2019/5/9
# author:   lpf599

"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
import airflow
from datetime import datetime, timedelta


default_args = {
    "owner": "success_test",
    "depends_on_past": False,
    "start_date": airflow.utils.dates.days_ago(1),  # 开始时间最好写昨日
    "email": ["xxxi@x.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 4,
    "retry_delay": timedelta(minutes=5),
    'max_active_runs': 16
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG("hello", default_args=default_args, schedule_interval="00 09 * * *")


def print_context(ds, **kwargs):
    print(kwargs)
    print(ds)
    return 'Whatever you return gets printed in the logs' + " {}_{}".format(ds, kwargs)


def print_except(ds, **kwargs):
    # a = 1/0
    return 'test failure'


run_1 = PythonOperator(
    task_id='print_the_context',
    provide_context=True,
    python_callable=print_context,
    op_kwargs={'ds': 10, "xxx": [1, 2, 3]},
    dag=dag,
)

run_2 = PythonOperator(
    task_id='print_except',
    provide_context=True,
    python_callable=print_except,
    op_kwargs={'ds': 10},
    dag=dag,
)

success_email = EmailOperator(
    task_id='success_email',
    to=["xxx@xxx.com"],
    subject='Airflow: RA All Tasks Success !',
    html_content='<h1> Congratulation ! </h1>',
    dag=dag
)

# run_3 = BashOperator(task_id="print_date", bash_command="date", dag=dag)

run_1 >> run_2
run_2 >> success_email
# # t1, t2 and t3 are examples of tasks created by instantiating operators
# t1 = BashOperator(task_id="print_date", bash_command="date", dag=dag)
#
# t2 = BashOperator(task_id="sleep", bash_command="sleep 5", retries=3, dag=dag)
#
# templated_command = """
#     {% for i in range(5) %}
#         echo "{{ ds }}"
#         echo "{{ macros.ds_add(ds, 7)}}"
#         echo "{{ params.my_param }}"
#     {% endfor %}
# """
#
# t3 = BashOperator(
#     task_id="templated",
#     bash_command=templated_command,
#     params={"my_param": "Parameter I passed in"},
#     dag=dag,
# )
#
# t2.set_upstream(t1)
# t3.set_upstream(t1)
