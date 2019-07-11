
from configparser import ConfigParser
from airflow_code import schedule
# from s3 import downloaddata
# from spark import read_process

parser = ConfigParser()
parser.read('config.ini')

var1 = parser['airflow']['var1']
var2 = parser['airflow']['var2']
schedule(var1, var2)
