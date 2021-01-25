from pyspark.sql import SparkSession
import json
import yaml
import sys
from schema import JSON_SCHEMA
RESOURCE_PATH = './src/main/resources/'
CONFIG_FILE = 'config.yaml'
ENV = sys.argv[1]


def create_session(app_name,inner_partitions):
    spark = SparkSession \
        .builder \
        .appName(app_name) \
        .getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    spark.conf.set('spark.sql.session.timeZone', 'UTC')
    spark.conf.set('spark.sql.shuffle.partitions',inner_partitions)
    return spark


def parse_config():
    with open(RESOURCE_PATH+ CONFIG_FILE, 'rb') as file:
        conf_dict = yaml.load(file)[ENV]
    return conf_dict


def read_schema():
    return JSON_SCHEMA
