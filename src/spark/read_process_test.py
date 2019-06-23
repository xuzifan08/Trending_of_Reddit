import sys
import os
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext, functions
import pyspark.sql.functions as f
import configparser


def time_format(t):
	t = t.withColumn("created_utc",f.from_unixtime("created_utc","dd/MM/yyyy HH:MM:SS"))
	t = t.withColumn("retrieved_on",f.from_unixtime("retrieved_on","dd/MM/yyyy HH:MM:SS"))
	return t


def write_dataframe(df):
	df.write.format("com.databricks.spark.redshift") \
                       .option("forward_spark_s3_credentials","true") \
                        .option("url","jdbc:redshift://examplecluster.c1rb4h53znme.us-east-1.redshift.amazonaws.com:5439/dev?user=awsuser&password=Fanfanmama123") \
                         .option("dbtable", "{}".format(df)) \
                          .option("tempdir", "s3n://fanredshift/") \
                           .mode("error") \
                            .save()


def main(sc):
	"""Read submission and comment json file from s3"""
	submission = SparkSession(sc).read.json("s3a://insightredditdata/redditsubmission/RS_2014-01")
        comment = SparkSession(sc).read.json("s3a://insightredditdata/redditcomment/RC_2014-01")
	
	"""Convert unix timestamp to human readable time"""
	time_format(submission)
	time_format(comment)
	
	"""Extract author, subreddit from submission and remove duplicates"""
	authors= submission[['author']] \
                    .union(comment[['author']]) \
                    .dropDuplicates()
	subreddit = submission[['subreddit', 'subreddit_id']] \
                       .union(comment[['subreddit', 'subreddit_id']]) \
                       .dropDuplicates())
	
	"""Extract submission and comment"""
	selectS =['author','created_utc','name','num_comments','score','subreddit_id','title']
	submission_w = submission[selectS]
	selectC =['author','body','created_utc','link_id','name','parent_id','score','subreddit_id']
	comment_w = comment[selectC]
	
	"""Clean column names"""
	submission_w = submission_w.withColumnRenamed("name","submission_id") \
                                    .withColumnRenamed("created_utc","time")
	comment_w = comment_w.withColumnRenamed("name","comment_id") \
                              .withColumnRenamed("link_id","submission_id") \
                                .withColumnRenamed("created_utc","time")
	
	"""Generate unique id for author"""
	authors = authors.withColumn("author_id", f.monotonically_increasing_id())
	
	"""Attach author_id to submisson and comment"""
	submission_w = submission_w.join(authors,submission_w.author == authors.author,"inner") \
                                  .drop("author")
	comment_w = comment_w.join(authors,comment_w.author == authors.author,"inner") \
                                .drop("author")
	
	"""Write dataframe to redshift"""
	write_dataframe(submission_w)
	write_dataframe(comment_w)
	write_dataframe(authors)
	write_dataframe(subreddit)


if __name__ == '__main__':
	"""
    	Setting up Spark session and Spark context, AWS access key
    	"""
    	config = configparser.ConfigParser()
    	config.read(os.path.expanduser('~/.aws/credentials'))
    	access_id = config.get('default', "aws_access_key_id")
    	access_key = config.get('default', "aws_secret_access_key")
    	spark = SparkSession.builder \
        	.appName("TrendingReddit") \
        	.config("spark.executor.memory", "1gb") \
        	.getOrCreate()

    	sc=spark.sparkContext
   	hadoop_conf=sc._jsc.hadoopConfiguration()
    	hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    	hadoop_conf.set("fs.s3a.access.key", access_id)
    	hadoop_conf.set("fs.s3a.secret.key", access_key)
    
    	main(sc)


