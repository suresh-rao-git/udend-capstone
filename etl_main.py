import re
import fnmatch, os
from pyspark.sql import SparkSession
import pandas as pd
import configparser
import boto3

from datetime import datetime, timedelta
from pyspark.sql.functions import udf, monotonically_increasing_id
from  pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

def create_spark_session():
    """
        Create spark session
    Returns:
        Spark session
    """
    spark = SparkSession. \
        builder. \
        config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11"). \
        appName("Capstone ETL"). \
        enableHiveSupport(). \
        getOrCreate()

    print("Spark config {}".format(spark.sparkContext.getConf().getAll()))

    return spark;


def load_static_data(spark, config, dfs):
    """
        CVS files for countries, ports, modes, visatype and visacodes, this
        data is populated from SAS data descriptions
    Args:
        spark:   Spark session to use
        config:  Config object which holds all configuration
        dfs:     Dictionary to hold dataframe created for static data

    Returns:
        None

    """

    filename = config['DATA']['INPUT_DIR'] + "/" + config['DATA']['COUNTRIES_FILE']
    print("Loading countries from " + filename)
    df_countries = spark.read.format("csv"). \
        option("sep", ","). \
        option("inferSchema", "true"). \
        option("header", "true"). \
        load(filename)
    df_countries.printSchema();


    filename = config['DATA']['INPUT_DIR'] + "/" + config['DATA']['PORTS_FILE']
    print("Loading countries from " + filename)
    df_ports = spark.read.format("csv"). \
        option("sep", ","). \
        option("inferSchema", "true"). \
        option("header", "true"). \
        load(filename)
    df_ports.printSchema()

    filename = config['DATA']['INPUT_DIR'] + "/" + config['DATA']['VISATYPES_FILE']
    print("Loading countries from " + filename)
    df_visatypes = spark.read.format("csv"). \
        option("sep", ","). \
        option("inferSchema", "true"). \
        option("header", "true"). \
        load(filename)
    df_visatypes.printSchema()

    filename = config['DATA']['INPUT_DIR'] + "/" + config['DATA']['VISACODES_FILE']
    print("Loading countries from " + filename)
    df_visacodes = spark.read.format("csv"). \
        option("sep", ","). \
        option("inferSchema", "true"). \
        option("header", "true"). \
        load(filename)
    df_visacodes.printSchema()

    filename = config['DATA']['INPUT_DIR'] + "/" + config['DATA']['MODES_FILE']
    print("Loading countries from " + filename)
    df_modes = spark.read.format("csv"). \
        option("sep", ","). \
        option("inferSchema", "true"). \
        option("header", "true"). \
        load(filename)
    df_modes.printSchema()

    print("Dataframe created for static data  ")
    dfs['countries'] = df_countries
    dfs['ports'] = df_ports
    dfs['visacodes'] = df_visacodes
    dfs['visatypes'] = df_visatypes
    dfs['modes'] = df_modes


@udf
def get_date(arrdate):
    """
    User Defined function for parsing date.
    Args:
        arrdate:  No of days from Jan 1st 1060.

    Returns:
        DateTime object arrival data
    """
    return (datetime(1960, 1, 1) + timedelta(arrdate))


def get_filenames(directory, pattern):
    """
        Get all files matching pattern under a directory
    Args:
        directory: Directory to find files
        pattern: Pattern match to find files

    Returns:
        List of matched files

    """

    selectFiles = []
    print("Get all files in directory {} for pattern {} ".format(directory, pattern))
    for root, dirs, files in os.walk(directory):
        for filename in fnmatch.filter(files, pattern):
            selectFiles.append(os.path.join(root, filename))

    print("Returning {} files for pattern match ".format((selectFiles)))
    return selectFiles


def create_static_parquet_files(spark, config, dataframes):
    output_file = config['DATA']['OUTPUT_DIR'] + "/countries"
    print("Creating parquet files for countries {} ".format(output_file))
    dataframes['countries'].write.mode("overwrite").parquet(output_file)

    output_file = config['DATA']['OUTPUT_DIR'] + "/ports"
    print("Creating parquet files for countries {} ".format(output_file))
    dataframes['ports'].write.mode("overwrite").parquet(output_file)

    output_file = config['DATA']['OUTPUT_DIR'] + "/visacodes"
    print("Creating parquet files for countries {} ".format(output_file))
    dataframes['visacodes'].write.mode("overwrite").parquet(output_file)

    output_file = config['DATA']['OUTPUT_DIR'] + "/visatypes"
    print("Creating parquet files for countries {} ".format(output_file))
    dataframes['visatypes'].write.mode("overwrite").parquet(output_file)

    output_file = config['DATA']['OUTPUT_DIR'] + "/modes"
    print("Creating parquet files for countries {} ".format(output_file))
    dataframes['modes'].write.mode("overwrite").parquet(output_file)

    output_file = config['DATA']['OUTPUT_DIR'] + "/airport"
    print("Creating parquet files for countries {} ".format(output_file))
    dataframes['modes'].write.mode("overwrite").repartitionBy("airport_code", "state").parquet(output_file)



def handle_airport_codes(spark, config, dataframes):
    """
        Cleans up airport-codes_csv.csv for processing as part of dimension data

    Args:
        spark:  Spark Session object
        config: Configuration object
        dataframes: Dataframes for static data

    Returns:

    """

    airportSchema = StructType([
                        StructField("airport_id",StringType()),
                        StructField("type",StringType()),
                        StructField("name",StringType()),
                        StructField("elevation_ft",StringType()),
                        StructField("continent",StringType()),
                        StructField("iso_country",StringType()),
                        StructField("iso_region",StringType()),
                        StructField("municipality",StringType()),
                        StructField("gps_code",StringType()),
                        StructField("iata_code",StringType()),
                        StructField("local_code",StringType()),
                        StructField("coordinates",StringType())
                        ])

    df = spark.read.load("us-cities-demographics.csv", format="csv", sep=",", header="true", schema=airport_code_schema)
    df = df.na.fill({'iata_code': '', 'local_code': ''})
    df_airport_clean = df.where( " iata_code != '' and local_code != '' " )
    df_airport_clean.printSchema()
    dataframes['airport_clean'] = df_airport_clean


def process_data_checks(spark, config, dataframes):


    df_imm_count = spark.sql("""
            select i94data.res as residence_country, i94data.gender  as gender, i94data.i94mode as mode,  i94data.visatype, i94data.visacode
            from i94data group by  i94data.res, i94data.gender
        """)

    print( "Display how many people immigrated by which type of visa based on gender")
    df_imm_count.show(10)


    print("Joining various dimension table to get full data ")
    df_i94_data = spark.sql("""
                        select i94.id as id , 
                               i94.arrival_date as arrival_date,
                               i94.i94yr as year,
                               i94.i94mon as month,
                               i94.i94cit as birth_country,
                               i94.i94res as residence_country,
                               i94.i94port as port_code,
                               ports.city as port_city,
                               ports.state as port_state,
                               i94.i94mode as mode,
                               mo.name as mode_name,
                               i94.i94visa as visa,
                               vc.name as visa_name,
                               i94.visatype as visatype_code,
                               vt.name as visatype_name,
                               i94.i94addr as staying_city,
                               i94.depdate as departure_date,
                               i94.visapost as issued_city,
                               i94.entdepa as arrival_flag,
                               i94.entdepd as departure_flag,
                               i94.entdepu as update_flag,
                               i94.biryear as birth_year,
                               i94.gender as gender,
                               i94.insnum as ins_number,
                               i94.airline as airline
                               from i94data i94
                               left join mo on i94.i94mode = mo.code
                               left join vc on i94.i94visa = vc.code
                               left join vt on i94.visatype = vt.code
                               left join ports on i94.i94port = ports.code

                        """)

    dataframes['df_i94_data'] = df_i94_data

    print("Created cleaned version of i94 data for file {} ".format(fileName))
    df_i94_data.printSchema()


def handle_i94data(spark, config, dataframes):
    """
        Handles i94 data, cleans the data and joins with
        static data and writes into parquet format
    Args:
        spark:  Spark Session object
        config: Configuration object
        dataframes: Dataframes for static data

    Returns:

    """
    dataframes['modes'].createOrReplaceTempView("mo")
    dataframes['countries'].createOrReplaceTempView("countries")
    dataframes['ports'].createOrReplaceTempView("ports")
    dataframes['visacodes'].createOrReplaceTempView("vc")
    dataframes['visatypes'].createOrReplaceTempView("vt")
    dataframes['airport_clean'].createOrReplaceTempView("air_code")

    fileNames = get_filenames(config['DATA']['DATA_DIR'], config['DATA']['SAS_FILE_PATTERN'])

    for fileName in fileNames:
        month = fileName[-18:-15]
        year = fileName[-15:-13]

        print("Processing i94 data from file " + fileName)
        df = spark.read.format('com.github.saurfang.sas.spark').load(fileName)

        print("Cleaning up null values for i94data ")
        df.na.fill({
            'arrdate': '0',
            'i94yr': '0',
            'i94port': '0',
            'i94mode': '0',
            'i94visa': '0',
            'visatype': '0'
        })

        print("Adding Id and arrival_date columns for i94data ")
        df = df.withColumn("id", monotonically_increasing_id())
        df = df.withColumn("arrival_date", get_date(df.arrdate))

        print("Joining data with static tables to create clean version of i94 data")
        df.printSchema()

        df.createOrReplaceTempView("i94data")

        print("Doing joins and data queries ")
        process_data_checks(spark, config, dataframes)

        output_file = config['DATA']['OUTPUT_DIR'] + "/i94_data_" + month + "_" + year
        print("Creating parquet files for cleaned i94 data {} ".format(output_file))

        dataframes['df_i94_data'].write.mode("append"). \
            partitionBy("year", "month"). \
            parquet(output_file)

        output_file = config['DATA']['OUTPUT_DIR'] + "/i94_data_by_port_" + month + "_" + year
        print("Creating parquet files for cleaned i94 data {} ".format(output_file))

        dataframes['df_i94_data'].write.mode("append"). \
            partitionBy( "port_city", "port_state"). \
            parquet(output_file)


def upload_files(config):
    session = boto3.Session(
        aws_access_key_id=config['AWS']['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=config['AWS']['AWS_SECRET_ACCESS_KEY'],
        region_name=config['AWS']['AWS_REGION']
    )
    s3 = session.resource('s3')
    bucket = s3.Bucket(config['AWS']['S3_BUCKET_NAME'])

    for subdir, dirs, files in os.walk(config['DATA']['OUTPUT_DIR']):
        for file in files:
            full_path = os.path.join(subdir, file)
            with open(full_path, 'rb') as data:
                bucket.put_object(Key=full_path[len(path)+1:], Body=data)

def main():
    config = configparser.ConfigParser()
    config.read('dl.cfg')

    dataframes = {}
    spark = create_spark_session()
    load_static_data(spark, config, dataframes)
    handle_airport_codes(spark, config, dataframes)
    handle_i94data(spark, config, dataframes)
    create_static_parquet_files(spark, config, dataframes)


if __name__ == '__main__':
    main()
