from pyspark.sql import SparkSession
from pyspark.sql.functions import *

import time
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession

from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier, FMClassifier
from pyspark.ml.classification import RandomForestClassifier,  LogisticRegression , NaiveBayes, MultilayerPerceptronClassifier
from pyspark.ml.feature import VectorAssembler, StringIndexer, VectorIndexer, MinMaxScaler, IndexToString, OneHotEncoder
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import  CrossValidatorModel

from pyspark.ml.pipeline import PipelineModel
from pyspark.sql.functions import col
import preperation as Pp

# def init():
#     KAFKA_TOPIC_NAME_CONS = 'final_prj'
#     KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'
#     # model = PipelineModel.load("D:\ITfiles\PythonFiles\Spark\prj\model")
#     model = CrossValidatorModel.read().load("D:\ITfiles\PythonFiles\Spark\prj\model")
#     return KAFKA_TOPIC_NAME_CONS, KAFKA_BOOTSTRAP_SERVERS_CONS, model

if __name__ == "__main__":
    print("Stream Data Processing Application Started ...")

    spark = SparkSession \
        .builder \
        .appName("PySpark Structured Streaming with Kafka and Message Format as JSON") \
        .master("local[*]") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")

    KAFKA_TOPIC_NAME_CONS = 'final_prj_5'
    KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'
    # model = PipelineModel.load("D:\ITfiles\PythonFiles\Spark\prj\model")
    model = CrossValidatorModel.read().load("D:\ITfiles\PythonFiles\Spark\prj\model")

    orders_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS) \
        .option("subscribe", KAFKA_TOPIC_NAME_CONS) \
        .option("startingOffsets", "latest") \
        .load()
    
    # orders_df = orders_df \
    # .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    # .writeStream \
    # .outputMode("append") \
    # .format("console") \
    # .start()
    
    # print("Printing Schema of data: ")

    orders_df1 = orders_df.selectExpr("CAST(value AS STRING)", "timestamp")
    
    orders_schema_string = '''index STRING,
        id STRING,
        click INT,
        hour STRING,
        C1 INT,
        banner_pos INT,
        site_id STRING,
        site_domain STRING,
        site_category STRING,
        app_id STRING,
        app_domain STRING,
        app_category STRING,
        device_id STRING,
        device_ip STRING,
        device_model STRING,
        device_type INT,
        device_conn_type INT,
        C14 INT,
        C15 INT,
        C16 INT,
        C17 INT,
        C18 INT,
        C19 INT,
        C20 INT,
        C21 INT'''

    orders_df2 = orders_df1\
        .select(from_csv(col("value"), orders_schema_string).alias("orders"),"timestamp")

    orders_df3 = orders_df2.select("orders.*", "timestamp")
    
    orders_df3.printSchema()

    # print(type(orders_df3))
    # print(dir(orders_df3))
    # print(type(orders_df3.select("C15")))
    # time.sleep(5)

    orders_df4 = Pp.preperation(orders_df3)

    # print("Chua transform")
    prediction1 = model.transform(orders_df4)
    # print("Da transform")
    predicted1 = prediction1.select('id','y', "prediction",'timestamp')
    # print("Khong  van de 1")
    predicted2 = prediction1.select('id','y','prediction')
    # print("Khong  van de 2")
    
    orders_agg_write_stream1 = predicted2 \
        .writeStream \
        .trigger(processingTime = "5 seconds")\
        .outputMode("append") \
        .option("path", "D:/ITfiles/PythonFiles/Spark/prj/output")\
        .option("checkpointLocation", "/user/kafka_stream_test_out/chk") \
        .format("csv") \
        .start()
    # print("Khong  van de 3")

    orders_agg_write_stream = predicted1 \
        .writeStream \
        .trigger(processingTime='5 seconds') \
        .outputMode("update") \
        .option("truncate", "false")\
        .format("console") \
        .start()
    # print("Khong  van de 4")

    orders_agg_write_stream1.awaitTermination()  
    orders_agg_write_stream.awaitTermination()

    print("Stream Data Processing Application Completed.")