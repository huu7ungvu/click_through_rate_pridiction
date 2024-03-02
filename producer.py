from kafka import KafkaProducer
from datetime import datetime
import time
import random
import pandas as pd 
import numpy as np

def init():
    path = "D:\ITfiles\PythonFiles\Spark\prj\data_stream.csv"
    data_stream = pd.read_csv(path)
    KAFKA_TOPIC_NAME_CONS = 'final_prj_5'
    KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'
    return data_stream, KAFKA_TOPIC_NAME_CONS, KAFKA_BOOTSTRAP_SERVERS_CONS

if __name__ == "__main__":
    print("Kafka Producer Application Started ... ")

    data_stream, KAFKA_TOPIC_NAME_CONS, KAFKA_BOOTSTRAP_SERVERS_CONS = init()

    kafka_producer_obj = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
                                       value_serializer=lambda x: x.encode('utf-8'))
    message_list = []
    message = None

    massage_total = []

    for i in range(1,100000):
        message_fields_value_list = []
        # message_fields_value_list.append(str(data_stream.iloc[i, 11]))
        message_fields_value_list.append(str(data_stream.iloc[i, 0]))
        message_fields_value_list.append(str(data_stream.iloc[i, 1]))
        message_fields_value_list.append(str(data_stream.iloc[i, 2]))
        message_fields_value_list.append(str(data_stream.iloc[i, 3]))
        message_fields_value_list.append(str(data_stream.iloc[i, 4]))
        message_fields_value_list.append(str(data_stream.iloc[i, 5]))
        message_fields_value_list.append(str(data_stream.iloc[i, 6]))
        message_fields_value_list.append(str(data_stream.iloc[i, 7]))
        message_fields_value_list.append(str(data_stream.iloc[i, 8]))
        message_fields_value_list.append(str(data_stream.iloc[i, 9]))
        message_fields_value_list.append(str(data_stream.iloc[i, 10]))
        message_fields_value_list.append(str(data_stream.iloc[i, 11]))
        message_fields_value_list.append(str(data_stream.iloc[i, 12]))
        message_fields_value_list.append(str(data_stream.iloc[i, 13]))
        message_fields_value_list.append(str(data_stream.iloc[i, 14]))
        message_fields_value_list.append(str(data_stream.iloc[i, 15]))
        message_fields_value_list.append(str(data_stream.iloc[i, 16]))
        message_fields_value_list.append(str(data_stream.iloc[i, 17]))
        message_fields_value_list.append(str(data_stream.iloc[i, 18]))
        message_fields_value_list.append(str(data_stream.iloc[i, 19]))
        message_fields_value_list.append(str(data_stream.iloc[i, 20]))
        message_fields_value_list.append(str(data_stream.iloc[i, 21]))
        message_fields_value_list.append(str(data_stream.iloc[i, 22]))
        message_fields_value_list.append(str(data_stream.iloc[i, 23]))
        message_fields_value_list.append(str(data_stream.iloc[i, 24]))
                                                 
        # message_fields_value_list.append(str(data_stream.iloc[i, 11]))
        message = ",".join(message_fields_value_list)
        print("Message Type: ", type(message))
        print("Message: ", message)
        kafka_producer_obj.send(KAFKA_TOPIC_NAME_CONS, message)
        kafka_producer_obj.flush()
        if i % 10 == 0:
            time.sleep(15)
    
    
    print("Kafka Producer Application Completed. ")