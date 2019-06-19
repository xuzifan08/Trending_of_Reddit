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

# convert unix timestamp to human readable time
submission = submission.withColumn("created_utc",f.from_unixtime("created_utc","dd/MM/yyyy HH:MM:SS"))
submission = submission.withColumn("retrieved_on",f.from_unixtime("retrieved_on","dd/MM/yyyy HH:MM:SS"))

# read comment data from s3
comment = spark.read.json("s3a://insightredditdata/redditcomment/RC_2014-01")

# convert unix timestamp to human readable time
comment = comment.withColumn("created_utc",f.from_unixtime("created_utc","dd/MM/yyyy HH:MM:SS"))
comment = comment.withColumn("retrieved_on",f.from_unixtime("retrieved_on","dd/MM/yyyy HH:MM:SS"))



# extract author, subreddit from submission and remove duplicates
author = submission[['author']].union(comment[['author']]).dropDuplicates()
subreddit = submission[['subreddit', 'subreddit_id']].union(comment[['subreddit', 'subreddit_id']]).dropDuplicates()



# extract submission and comment
submission = submission[['author','created_utc','name','num_comments','score','subreddit_id','title']]
comment = comment[['author','body','created_utc','link_id','name','parent_id','score','subreddit_id']]



# rename id of submission and comment
submission = submission.withColumnRenamed("name", "submission_id").withColumnRenamed("created_utc","time")
comment = comment.withColumnRenamed("name", "comment_id").withColumnRenamed("link_id", "submission_id").withColumnRenamed("created_utc","time")

# attach author_id to submisson and comment
submission_new = submission.join(author, submission.author == author.author,"inner")
comment_new = comment.join(author, comment.author == author.author,"inner")



# generate unique id for author
author = author.withColumn("author_id", f.monotonically_increasing_id())





# test if code is working
submission.printSchema()
comment.printSchema()
subreddit.printSchema()
author.printSchema()



# write data into redshift
#submission.write.format("com.databricks.spark.redshift").option("url", "jdbc:redshift://examplecluster.c1rb4h53znme.us-east-1.redshift.amazonaws.com:5439/dev?user=awsuser&password=Fanfanmama123").option("dbtable", "submission").option("tempdir", "s3n://fanredshift/").mode("error").save()

# try to write dataframe to s3
#submission.write.mode('append').json("s3n://zifanzifan/")


# write dataframe to csv in s3
#submission.write.save("s3://insightredditdata/submission_test.csv", format='csv', header=True)
