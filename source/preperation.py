# tách cột hour ra 4 cột mới
from pyspark.sql.functions import month, dayofweek, dayofmonth, hour
from pyspark.sql.functions import col, when

def preperation (train):
    # create some feature from hour 
    train = train.withColumn("month", month("hour"))
    train = train.withColumn("dayofweek", dayofweek("hour"))
    train = train.withColumn("day", dayofmonth("hour"))
    train = train.withColumn("hour_time", hour("hour"))

    # dealing with outliers by capping
    # cols = ['C15', 'C16', 'C19', 'C21']
    # for col_name in cols:
    #     print(col_name)
    #     quantile_val = train.approxQuantile(col_name, [0.98], 0.25)[0]
    #     if quantile_val < 0.5 * train.select(col_name).rdd.max()[0]:
    #         train = train.withColumn(col_name, when(col(col_name) >= quantile_val, quantile_val).otherwise(col(col_name)))
    
    # drop feature (xem xét có bỏ id không nhé tại vì cần id để show)
    # train = train.drop("id")
    train = train.drop("hour").drop("index")
    train = train.withColumnRenamed("click", "y").withColumnRenamed("hour_time", "hour")

    return train