import __main__

from os import environ, listdir, path
import json
from pyspark import SparkFiles
from pyspark.sql import SparkSession

def start_spark(app_name='hellofresh', master='local[*]', jar_packages=[],files=[], spark_config={}):
    """Start Spark session, get Spark logger and load config files."""
    spark_builder = (SparkSession.builder.master(master).appName(app_name))

    # create Spark JAR packages string
    spark_jars_packages = ','.join(list(jar_packages))
    spark_builder.config('spark.jars.packages', spark_jars_packages)

    spark_files = ','.join(list(files))
    spark_builder.config('spark.files', spark_files)

    # add other config params
    for key, val in spark_config.items():
        spark_builder.config(key, val)

    # create session and retrieve Spark logger object
    spark_sess = spark_builder.getOrCreate()
    conf = spark_sess.sparkContext.getConf()
    app_id = conf.get('spark_sess.app.id')
    logger = spark_sess._jvm.org.apache.log4j
    message_prefix = '<' + app_name +  '>'
    #logger = logging.Log4j(spark_sess)
    spark_logger=logger.LogManager.getLogger(message_prefix)

    # get config file if sent to cluster with --files
    spark_files_dir = SparkFiles.getRootDirectory()
    config_files = [filename
                    for filename in listdir(spark_files_dir)
                    if filename.endswith('hellofresh.json')]

    if config_files:
        path_to_config_file = path.join(spark_files_dir, config_files[0])
        with open(path_to_config_file, 'r') as config_file:
            config_dict = json.load(config_file)
        spark_logger.warn('loaded config from ' + config_files[0])
    else:
        spark_logger.warn('no config file found')
        config_dict = None

    return spark_sess, spark_logger, config_dict

