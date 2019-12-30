import pandas as pd
import re
from pyspark.sql import SparkSession
import os
import glob
import configparser
from datetime import datetime, timedelta
from pyspark.sql import types as t
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear


def createSparkSession():
    spark = SparkSession \
        .builder\
        .config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11")\
        .enableHiveSupport().getOrCreate()
    return spark;


# --------------------------------------------------------
def parse_input_files(PATHS, path, extension, start_str):
    """Parse recursively all input files from given directory path.

    Keyword arguments:
    * PATHS     -- PATHS variable with all.
    * path      -- path to parse.
    * extension -- file extension to look for.

    Output:
    * all_files -- List of all valid input files found.
    """
    print(f"PATH: {path}")
    print(f"EXTENSION: {extension}")
    # Get (from directory) the files matching extension
    all_files = []
    for root, dirs, files in os.walk(path):
        files = glob.glob(os.path.join(root, extension))
        for f in files :
            all_files.append(os.path.abspath(f))

    return all_files



def main():

    start_str = datetime.now().strftime('%Y-%m-%d-%H-%M-%S-%f')
    print( start_str )

    input_files = parse_input_files({}, \
                                    "D://work//Udacity/", \
                                    "*.sas7bdat", \
                                    start_str)

    for file in input_files:
        print ( "File Name {}".format(file))



if __name__ == "__main__":
    main()