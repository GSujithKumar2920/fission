# Databricks notebook source
from pyspark.sql import SparkSession, DataFrame

class Reader:
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session
    
    def read_from_json(self, **config):
        options = config['options']
        path = config['path']
        return spark.read.options(**options).json(path)
      
    def read_from_sql(self, **config):
        options = config['options']
        return spark.read.format('jdbc').options(**options).load()
             
    def read(self, source_type, **config):
        if source_type == 'json':
            return self.read_from_json(**config)
        elif source_type == 'sql':
            return self.read_from_sql(**config)

        
class Writer:
    def __init__(self, df: DataFrame):
        self.df = df
    
    def write_to_json(self, **config):
        self.df.write.mode('overwrite').json(config['path'])
    
    def write_to_sql(self, **config):
        self.df.write.format('jdbc').mode('overwrite').options(**options).save()
        
    def write(self, dest_type, **config):
        if dest_type == 'json':
            self.write_to_json(**config)
        elif dest_type == 'sql':
            self.write_to_sql(**config)

# COMMAND ----------

# MAGIC %md  
# MAGIC # JSON READ/WRITE

# COMMAND ----------

# READ
# json_read_options = {
# #                 'timeZone'                 :'',
#                 'multiline'                :'true', 
#                 'prefersDecimal'           :'true',
#                 'primitivesAsString    '      :'false',
#                 'allowComments'            :'false',
#                 'allowUnquotedFieldNames'  :'false',
#                 'allowSingleQuotes'        :'true',
#                 'allowNumericLeadingZero'  :'false',
# #                 'columnNameOfCorruptRecord':
#                 'dateFormat'               :'yyyy-MM-dd',
#                 'timestampFormat'          :'yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]',
#                 'allowUnquotedControlChars':'false',
# #                 'encoding'                 :Detected automatically when multiLine is set to true (for reading)
#                 'lineSep'                  :'\r, \r\n, \n',
#                 'samplingRatio'            :'1.0',
#                 'dropFieldIfAllNull'       :'false',
#                 'locale'                   :'en-US',
#                 'allowNonNumericNumbers'   :'true',
#                 'mode'                     :'PERMISSIVE'/'DROPMALFORMED'/'FAILFAST',
#                 'allowBackslashEscapingAnyCharacter'  :'false',
#         }

reader = Reader(spark)
options = {'multiline':'true', 'prefersDecimal':'true'}
df = reader.read('json', path='/FileStore/shared_uploads/hindu.varma@fissionlabs.com/sample_json.json', options=options)

# Transform
from pyspark.sql.functions import col
from pyspark.sql.types import DateType

df = df.withColumn("registered",col("registered").cast(DateType()))

# COMMAND ----------

# Write
# json_write_options = {
# #                 'timeZone'                 :'',
#                 'dateFormat'               :'yyyy-MM-dd',
#                 'timestampFormat'          :'yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]',
#                 'allowUnquotedControlChars':'false',
#                 'encoding'                 :'UTF-8' 
#                 'lineSep'                  :'\n',
#                 'ignoreNullFields'         :'false',
#                 'compression'              :'none/ bzip2/ gzip/ lz4/ snappy/ deflate'   
# }

writer = Writer(df)
writer.write('json', path='/FileStore/shared_uploads/hindu.varma@fissionlabs.com/sample_write')

# COMMAND ----------

# MAGIC %md
# MAGIC # SQL READ/WRITE

# COMMAND ----------

# Configure sqlserver in Azure and get the below details
host_name = "sujitestserver.database.windows.net"
database = "sqldb1" 
port = 1433 
url = f"jdbc:sqlserver://{host_name}:{port};database={database};"
driver = 'com.microsoft.sqlserver.jdbc.SQLServerDriver'
table_name = 'salesLT.Product'
username = "suji@sujitestserver" 
password = "ijus123***"

# COMMAND ----------

# Read data from sql
additional_options = {'partitionColumn' : 'name of the column used for partitioning',
                     'numPartitions': 'max no of partitions',
                     'lowerBound':'min number of rows in each partition',
                     'upperBound':'max no of rows in each partition',
                     'batchsize ':'to boost writing speed',
                     'fetchsize':'to boost reading speed'}

reader = Reader(spark)
# options can have query but query and dbtable cannot be specified at the same time
# 'query' :f"select * from {table_name}"
options = {'url':url, 'driver':driver, 'dbtable' :table_name, 'user':username, 'password': password}
df = reader.read('sql', options=options)
df.display()

# COMMAND ----------

# Write data to sql
writer = Writer(df)
table_name = 'salesLT.Product_write'
options = {'url':url, 'driver':driver, 'dbtable' :table_name, 'user':username, 'password': password}
writer.write('sql', options=options)

# COMMAND ----------

# MAGIC %md
# MAGIC # S3 READ/WRITE

# COMMAND ----------

aws_region = "ap-northeast-1"
# Keys generated on AWS console
access_key = "AKIASEIMLQJJYLNHTE3P"
secret_key = "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"


# COMMAND ----------

# Configure S3
def configure_s3(access_key, secret_key, aws_region):
    
    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)

    # If you are using Auto Loader file notification mode to load files, provide the AWS Region ID.
    sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3." + aws_region + ".amazonaws.com")

# COMMAND ----------



# COMMAND ----------

configure_s3(access_key, secret_key, aws_region)

# COMMAND ----------

# Read data from s3
reader = Reader(spark)
options = {'multiline':'true', 'prefersDecimal':'true'}
df = reader.read('json', path='s3://suji-9-1/data-json/sample_json.json', options=options)
df.count()

# COMMAND ----------

# Write data to s3
writer = Writer(df)
writer.write('json', path='s3://suji-9-1/data-json-write/')
