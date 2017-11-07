import psycopg2
import sys, os, configparser
from pyspark import SparkConf, SparkContext

cred_config = configparser.ConfigParser()
cred_config.read(os.path.expanduser("~/.aws/credentials"))
access_id = cred_config.get("default", "aws_access_key_id") 
access_key = cred_config.get("default", "aws_secret_access_key") 

rds_config = configparser.ConfigParser()
rds_config.read(os.path.expanduser("~/config"))
rds_database = rds_config.get("default", "database") 
rds_user = rds_config.get("default", "user")
rds_password = rds_config.get("default", "password")
rds_host = rds_config.get("default", "host")
rds_port = rds_config.get("default", "port")

log_path = "/home/hadoop/logs/" # don't change this
aws_region = "us-east-1"  # don't change this
s3_bucket = "cs327e-fall2017-final-project" # don't change this

tags_file = "s3a://" + s3_bucket + "/movielens/tags.csv"
links_file = "s3a://" + s3_bucket + "/movielens/links.csv"

os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages=org.apache.hadoop:hadoop-aws:2.7.3 pyspark-shell"

# global variable sc = Spark Context
sc = SparkContext()

sc.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")
hadoop_conf=sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("com.amazonaws.services.s3.enableV4", "true")
hadoop_conf.set("fs.s3a.access.key", access_id)
hadoop_conf.set("fs.s3a.secret.key", access_key)

def print_rdd(rdd, logfile):
  f = open(log_path + logfile, "w") 
  results = rdd.collect() 
  counter = 0
  for result in results:
    counter = counter + 1
    f.write(str(result) + "\n")
    if counter > 30:
      break
  f.close()

def parse_line(line):

  # Add logic for parsing the line (step 3)
  
  return (movie_id, tag)

lines = sc.textFile(tags_file)
rdd_tags = lines.map(parse_line) # movie_id, tag
print_rdd(rdd_tags, "movie_tag_pairs")

# Add logic for distinct (step 4)

rdd_distinct_tags.cache()

################## links file ##################################

def parse_links_line(line):
  fields = line.split(",")
  movie_id = int(fields[0])
  imdb_id = int(fields[1])
  return (movie_id, imdb_id)
  
# lookup imdb id
links_lines = sc.textFile(links_file)
rdd_links = links_lines.map(parse_links_line) # movie_id, imdb_id
#print_rdd(rdd_links, "rdd_links")

# Add logic for joining rdd_links and rdd_distinct_tags (step 5)
print_rdd(rdd_joined, "movielens_imdb_joined")

def add_imdb_id_prefix(tupl):
  movielens_id, atupl = tupl
  tag, imdb_id = atupl
  imdb_id_str = str(imdb_id)
  
  if len(imdb_id_str) == 1:
     imdb_id_str = "tt000000" + imdb_id_str
  elif len(imdb_id_str) == 2:
     imdb_id_str = "tt00000" + imdb_id_str
  elif len(imdb_id_str) == 3:
     imdb_id_str = "tt0000" + imdb_id_str
  elif len(imdb_id_str) == 4:
     imdb_id_str = "tt000" + imdb_id_str
  elif len(imdb_id_str) == 5:
     imdb_id_str = "tt00" + imdb_id_str
  elif len(imdb_id_str) == 6:
     imdb_id_str = "tt0" + imdb_id_str
  else:
     imdb_id_str = "tt" + imdb_id_str
     
  return (imdb_id_str, tag)

# add the "tt0" prefix to imdb_id 
formatted_rdd = rdd_joined.map(add_imdb_id_prefix) 
print_rdd(formatted_rdd, "formatted_rdd")

def save_to_db(list_of_tuples):
    conn = psycopg2.connect(database=rds_database, user=rds_user, password=rds_password, host=rds_host, port=rds_port)
    conn.autocommit = True
    
    # Add logic to extract each element (step 7)
    
    try:
        # Add logic to perform insert statement (step 7)
    
    except Exception as e:
        print "Error in save_to_db: ", e.message
  
formatted_rdd.foreachPartition(save_to_db)

# free up resources
sc.stop()
