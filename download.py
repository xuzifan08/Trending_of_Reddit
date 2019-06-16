import boto3
import os
from os.path import join

S3_BUCKET = 'insightredditdata'
S3_FOLDER = 'redditcomment'
URL_PREFIX = 'https://files.pushshift.io/reddit/comments/'
FILENAME_SUFFIX = 'bz2'
FILENAMES = ['RC_2011-01','RC_2011-02','RC_2011-02','RC_2011-03','RC_2011-04','RC_2011-05','RC_2011-06','RC_2011-07','RC_2011-08','RC_2011-09','RC_2011-10','RC_2011-11','RC_2011-12']


s3 = boto3.client('s3')
for filename in FILENAMES:
        zip_name = '{}.{}'.format(filename, FILENAME_SUFFIX)
        url = join(URL_PREFIX, zip_name)
        os.system('wget {}'.format(url))
        os.system('bunzip2 {}'.format(zip_name))
        s3.upload_file(Filename=filename, Bucket=S3_BUCKET, Key=join(S3_FOLDER, filename))
        os.system('rm {}'.format(filename))
~                                                                                                                    
~         
