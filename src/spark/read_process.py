import sys
from pyspark.sql.types import StringType, ArrayType, FloatType, IntegerType
import os
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext, functions
import pyspark.sql.functions as f


def time_format(t):
	"""convert unix time to human readable time
	"""
	t.withColumn("created_utc",f.from_unixtime("created_utc","dd/MM/yyyy HH:MM:SS"))
	t.withColumn("retrieved_on",f.from_unixtime("retrieved_on","dd/MM/yyyy HH:MM:SS"))


def write_dataframe(df,dbname):
	"""write cleaned dataframe to redshift
	"""
	df.write.format("com.databricks.spark.redshift") \
                       .option("forward_spark_s3_credentials","true") \
                        .option("url","jdbc:redshift://examplecluster.c1rb4h53znme.us-east-1.redshift.amazonaws.com:5439/dev?user=awsuser&password=Fanfanmama123") \
                         .option("dbtable", dbname) \
                          .option("tempdir", "s3n://fanredshift/") \
                           .mode("append") \
                            .save()


def add_str(str_a):
	"""add str and authorname to generate a new column called author_id
	"""
	strr = "t2_"+str_a
	return strr
	

def main(sc):
	spark = SparkSession.builder \
                 .appName("TrendingReddit") \
                 .config("spark.executor.memory", "1gb") \
                 .getOrCreate()

        sc=spark.sparkContext
	"""Read submission and comment json file from s3"""
	submission = SparkSession(sc).read.json("s3a://insightredditdata/redditsubmission/RS_2015-06")
        comment = SparkSession(sc).read.json("s3a://insightredditdata/redditcomment/RC_2015-06")
	
	
	"""filter out the deleted author"""
	submission = submission \
			.filter(submission.author!="[deleted]")
	comment = comment \
			.filter(comment.author!="[deleted]")

	"""filter out the title and body which is longer than the maximun length"""
	"""submission = submission \
                         .filter(size('title')<=256)
        comment = comment \
                         .filter(size('body')<=256)
	"""

	"""Convert unix timestamp to human readable time"""
	submission = submission \
   			.withColumn("created_utc",f \
			.from_unixtime("created_utc","dd/MM/yyyy HH:MM:SS"))
	submission =  submission \
                        .withColumn("retrieved_on",f \
			.from_unixtime("retrieved_on","dd/MM/yyyy HH:MM:SS"))
        comment = comment \
                        .withColumn("created_utc",f \
			.from_unixtime("created_utc","dd/MM/yyyy HH:MM:SS"))
        comment = comment \
                        .withColumn("retrieved_on",f \
			.from_unixtime("retrieved_on","dd/MM/yyyy HH:MM:SS"))

	"""Extract author, subreddit from submission and remove duplicates"""
	authors= submission[['author']] \
                    .union(comment[['author']]) \
                    .dropDuplicates()

	subreddit = submission[['subreddit', 'subreddit_id']] \
                       .union(comment[['subreddit', 'subreddit_id']]) \
                       .dropDuplicates()

	"""Extract submission and comment"""
	selectS =['author','created_utc','name','num_comments','score','subreddit_id']
	submission_w = submission[selectS]

	selectC =['author','created_utc','link_id','name','parent_id','score','subreddit_id']
	comment_w = comment[selectC]

	"""Clean column names"""
	submission_w = submission_w.withColumnRenamed("name","submission_id") \
                                    .withColumnRenamed("created_utc","time")

	comment_w = comment_w.withColumnRenamed("name","comment_id") \
                              .withColumnRenamed("link_id","submission_id") \
                                .withColumnRenamed("created_utc","time")

	"""Generate unique id for author"""
	add_str_udf = f \
			.udf(add_str, StringType())
	authors = authors.withColumn("author_id", add_str_udf('author'))#, f.lit(str_prefix)+authors['author'])
		
	"""Attach author_id to submisson and comment"""
	submission_w = submission_w.join(authors,submission_w.author == authors.author,"inner") \
                                  .drop("author")
	
	comment_w = comment_w.join(authors,comment_w.author == authors.author,"inner") \
                                .drop("author")

	"""Write dataframe to redshift"""
	write_dataframe(submission_w,"submission")
	write_dataframe(comment_w,"comment")
	write_dataframe(authors,"authors_temp")
	write_dataframe(subreddit,"subreddit_temp")


if __name__ == '__main__':
	"""
    	Setting up Spark session and Spark context, AWS access key
    	"""
    	main(sc)


