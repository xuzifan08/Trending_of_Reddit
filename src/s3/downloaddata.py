import boto3
import os
from os.path import join


def main(filenames,url_prefix,s3_folder):
	"""
	download compressed files, decompress it and upload to s3 bucket
	"""
	for filename in filenames:
		url = join(url_prefix,filename)
		os.system('wget {}'.format(url))
		new_filename, filename_suffix = filename.split(".")[:2]
		mapping = {'bz2':'bunzip2','xz':'unxz','zst':'unzstd'}
		os.system('{} {}'.format(mapping[filename_suffix], filename))
		s3.upload_file(Filename=newfilename, Bucket=S3_BUCKET, Key=join(s3_folder, newfilename))
		os.system('rm {}'.format(newfilename))


if __name__ == "__main__":
	#set connection
	s3 = boto3.client('s3')
	S3_BUCKET = 'insightredditdata'
	
	"""
	download submissions and comments
	"""
	url_prefix_subm = 'https://files.pushshift.io/reddit/submissions/'
	url_prefix_com = 'https://files.pushshift.io/reddit/comments/'	
        
        """
	all the files from 2014 till now
	"""
	filenames_subm = []
	filenames_com = []
	
	s3_folder_subm = 'redditsubmission'
	s3_folder_com = 'redditcomment' 

	main(filenames_subm,url_prefix_subm,s3_folder_subm)
	main(filenames_com,url_prefix_com,s3_folder_com)

