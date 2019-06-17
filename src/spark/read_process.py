import pyspark
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext, functions
import pyspark.sql.functions as f


sc = SparkContext.getOrCreate()
spark = SparkSession(sc)


# read submission data (monthly) from s3
submission = spark.read.json("s3a://insightredditdata/redditsubmission/RS_2014-01")

# select useful columns
selectS = ['author','created_utc','domain','id','name','num_comments','retrieved_on','score','subreddit','subreddit_id','title']
submission = submission[selectS]

# convert unix timestamp to human readable time
submission = submission.withColumn("created_utc",f.from_unixtime("created_utc","dd/MM/yyyy HH:MM:SS"))
submission = submission.withColumn("retrieved_on",f.from_unixtime("retrieved_on","dd/MM/yyyy HH:MM:SS"))



# read comment data from s3
comment = spark.read.json("s3a://insightredditdata/redditcomment/RC_2014-01")

# select useful columns: the link_id points to the id of submission
selectC = ['author','body','created_utc','id','link_id','name','parent_id','retrieved_on','score','subreddit','subreddit_id']
comment = comment[selectC]

# convert unix timestamp to human readable time
comment = comment.withColumn("created_utc",f.from_unixtime("created_utc","dd/MM/yyyy HH:MM:SS"))
comment = comment.withColumn("retrieved_on",f.from_unixtime("retrieved_on","dd/MM/yyyy HH:MM:SS"))



# test if code is working
submission.show()
comment.show()



# write data into redshift
submission.write.format("com.databricks.spark.redshift").option("url", "jdbc:redshift://examplecluster.c1rb4h53znme.us-east-1.redshift.amazonaws.com:5439/dev?user=awsuser&password=Fanfanmama123").option("dbtable", "submission").option("tempdir", "s3n://fanredshift/").mode("error").save()


