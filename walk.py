import fnmatch
import os

directory = 'data'
pattern = '*.sas7bdat'

selectFiles = []
print("Get all files in directory {} for pattern {} ".format(directory, pattern))
for root, dirs, files in os.walk(directory):
    for filename in fnmatch.filter(files, pattern):
        selectFiles.append(os.path.join(root, filename))

print("Returning {} files for pattern match ".format((selectFiles)))


print (selectFiles)



pyspark --packages saurfang:spark-sas7bdat:2.0.0-s_2.10

fname=".//data//2016//i94_jan16_sub.sas7bdat"
df=spark.read.format('com.github.saurfang.sas.spark').load(fname)

df.createOrReplaceTempView("i94")

df_vt=spark.read.format("csv").option("sep", ",").option("inferSchema", "true").option("header", "true").load( "input/vistypes.csv")
df_vt.createOrReplaceTempView("vt")


ac_df=spark.read.format("csv").option("sep", ",").option("inferSchema","true").option("header","true").load("D://work//Udacity//capstone_project//airport-codes_csv.csv")


val spark = SparkSession.builder().appName("Spark Hive Example").config("spark.sql.warehouse.dir", "./dwh").enableHiveSupport().getOrCreate()