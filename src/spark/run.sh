#!/bin/bash
spark-submit \
	--jars $SPARK_HOME/jars/aws-java-sdk-1.7.4.jar,$SPARK_HOME/jars/RedshiftJDBC42-no-awssdk-1.2.27.1051.jar,$SPARK_HOME/jars/spark-redshift_2.10-3.0.0-preview1.jar,$SPARK_HOME/jars/spark-avro_2.11-4.0.0.jar \
	--master spark://ip-10-0-0-11:7077 \
	--num-executors 3 
	read_process.py 
