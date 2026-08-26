from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.ml import Pipeline , PipelineModel
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import *
from pyspark.sql.types import *
import numpy as np

spark = (
    SparkSession
    .builder
    .master("local[*]")
    .appName("Stream Iot Data")
    .config("spark.cassandra.connection.host", "cassandra_iot1,cassandra_iot2,cassandra_iot3") 
    .config("spark.cassandra.connection.port", "9042")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.jars.packages",
            "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0,"
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
    .getOrCreate()
)

kafka_df = (
    spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "iot-kafka-1:29092,iot-kafka-2:29093,iot-kafka-3:29094")
    .option("subscribe", "iot_data")
    .option("startingOffsets", "earliest")
    .load()
)

#kafka_df.printSchema()

kafka_df = kafka_df.withColumns({
    "key":kafka_df.key.cast("string"),
    "value":kafka_df.value.cast("string")
})

iot_schema = StructType([
    StructField("device_id" , StringType() , nullable=True),
    StructField("timestamp", TimestampType() , nullable=True),
    StructField("temperature", DecimalType(20,4) , nullable=False),
    StructField("humidity", DecimalType(20,4) , nullable=False),
    StructField("battery_level", DecimalType(25,4) , nullable=False),
])

json_kafka = kafka_df.withColumn("json_value",from_json("value",iot_schema)).select("json_value.*")

classifier = PipelineModel.load('../models/classifier') 

from pyspark.sql.functions import monotonically_increasing_id

def call_classifier_model(batch_df, batch_id):
    if batch_df.isEmpty():
        return 
    
    # Prepare features for the classifier
    vector_assembler = VectorAssembler(inputCols=batch_df.columns[2:], outputCol="features")
    transformed_data = vector_assembler.transform(batch_df).select("features")
    
    # Get predictions
    predictions = classifier.transform(transformed_data).select("prediction")
    
    # Add row numbers to BOTH DataFrames
    batch_df_with_id = batch_df.withColumn("row_id", monotonically_increasing_id())
    predictions_with_id = predictions.withColumn("row_id", monotonically_increasing_id())
    
    # Join using row_id
    result = batch_df_with_id.join(predictions_with_id, on="row_id", how="inner")
    result = result.drop("row_id")
    result = result.withColumnRenamed("prediction", "anomaly")
    (
        result
        .write
        .format("org.apache.spark.sql.cassandra")
        .mode("append")
        .option("keyspace","iot_data")
        .option("table","sensor_data")
        .save()
    )
    print(f"Batch Classified and Saved Succesuly with taile {batch_df.count()} To Cassandra in batch id {batch_id}")


(
    json_kafka
    .writeStream
    .outputMode("append")
    .foreachBatch(call_classifier_model)
    .option("truncate",False)
    #.format("console")
    .trigger(processingTime="5 seconds")
    .option("checkpointLocation", "checkpoint_dir_kafka")
    .start()
    .awaitTermination()
)