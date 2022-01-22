import argparse

from dependencies.spark import start_spark
from utils.hellofreshutils  import cookTimeMinute
from pyspark.sql.functions import round,translate,lower,when,split,col,avg,regexp_replace
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

parser = argparse.ArgumentParser()
parser.add_argument("--inputFilePath", help="Path of Input File.")
parser.add_argument("--outputFilePath", help="Path of output File.")
parser.add_argument("--conf", help="Path of output File.")
args = parser.parse_args()
#if args.inputFilePath:
#    ipFilePath = args.inputFilePath
#if args.outputFilePath:
#    outFilePath = args.outputFilePath



def extract_data(spark,filePath):
    """Load data from json file format.
    :param spark: Spark session object.
    :param filePath: filePath of json
    :return: Spark DataFrame.
    """
    df = (
        spark
        .read
        .json(filePath+"/*.json"))

    return df


def transform_data(df, configObj):
    """Transform original dataset.
    :param df: Input DataFrame.
    :param steps_per_floor_: The number of steps per-floor at 43 Tanner
        Street.
    :return: Transformed DataFrame.
    """
    to_remove=configObj['stopwordslist']
    print("----------------"+",".join(to_remove))
    timeinmin = udf(cookTimeMinute, IntegerType())

    newDf=df.withColumn("cookTimeMinute",timeinmin("cookTime")).withColumn("prepTimeMinute",timeinmin("prepTime")).withColumn("totalTime",col("cookTimeMinute")+col("prepTimeMinute"))


    df_transformed=newDf.withColumn("bracketRemoved",regexp_replace(col("ingredients"), "\(.*?\)", '')).withColumn("ingrdnt_removedstopwords",regexp_replace(col("bracketRemoved"), "\(|\)|[0-9]|\/|-|,", '')).withColumn("ingrdnt_removedspclchar",regexp_replace(lower(col("ingrdnt_removedstopwords")), '|'.join(to_remove),'')).withColumn("listofingredients",translate(col("ingrdnt_removedspclchar"),"\n",",")).select("listofingredients","cookTimeMinute","totalTime").where("listofingredients like '%beef%'").withColumn("difficulty", when(newDf.totalTime <= 30,"easy").when((newDf.totalTime > 30) & (newDf.totalTime <= 60),"medium").when(newDf.totalTime > 60,"hard").otherwise("notknown"))


    finalDf=df_transformed.groupBy("difficulty").agg(round(avg("cookTimeMinute"),2).alias("avg_total_cooking_time"))
    return finalDf


def load_data(df,outputPath):
    """Collect data locally and write to CSV.
    :param df: DataFrame to print.
    :return: None
    """

    print(outputPath)
    

    (df
     .coalesce(1)
     .write
     .csv(outputPath+'/transformed_data', mode='overwrite', header=True))
    return None


#logging.debug("Input File Path {} and output File location {}".format(ipFilePath,outFilePath))



def main(args):
    """Main ETL script definition.
    :return: None
    """
    # start Spark application and get Spark session, logger and config
    spark, log, config = start_spark(
        app_name='helloFresh',
        files=[args.conf])

    # log that main ETL job is starting
    log.warn('etl_job is up-and-running')

    # execute ETL pipeline
    data = extract_data(spark,args.inputFilePath)
    data_transformed = transform_data(data,config)
    data_transformed.persist()
    data_transformed.count()

    load_data(data_transformed,args.outputFilePath)

    # log the success and terminate Spark application
    log.warn('test_etl_job is finished')
    spark.stop()
    return None


# entry point for PySpark ETL application
if __name__ == '__main__':
    main(args)

