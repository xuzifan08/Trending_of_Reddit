from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow import DAG
from datetime import datetime, timedelta

default_args = {
    'owner': 'yourself',
    'depends_on_past': False,
    'start_date': datetime(2019, 6, 24),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}
dag = DAG('ssh_tutorial', default_args=default_args, schedule_interval=timedelta(days=1))

SPARK_CMD = """spark-submit --jars \
	$SPARK_HOME/jars/aws-java-sdk-1.7.4.jar,\
        $SPARK_HOME/jars/RedshiftJDBC42-no-awssdk-1.2.27.1051.jar,\
        $SPARK_HOME/jars/spark-redshift_2.10-3.0.0-preview1.jar,\
        $SPARK_HOME/jars/spark-avro_2.11-4.0.0.jar \
        --master spark://{master_ip}:7077 \
        --num-executors {num_executors} \
        {spark_filepath}"""\
        .format(master_ip=master_ip, num_executors=num_executors, spark_filepath=spark_filepath)


t1 = SSHOperator(task_id='Download_data_to_s3', ssh_conn_id="ssh_spark_master", command="python3 /home/ubuntu/downloaddata.py", dag=dag)
t2 = SSHOperator(task_id='Read_transform_data_in_spark_and_store_data_in_redshift', ssh_conn_id="ssh_spark_master", command=SPARK_CMD, dag=dag)

t1.set_upstream(t2)
