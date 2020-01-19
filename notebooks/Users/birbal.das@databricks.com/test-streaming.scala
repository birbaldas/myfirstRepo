// Databricks notebook source
import org.apache.spark.sql._
import io.delta.tables._

// Reset the output aggregates table
Seq.empty[(Long, Long)].toDF("key", "count").write
  .format("delta").mode("overwrite").saveAsTable("ttaggregates")

val deltaTable = DeltaTable.forName("ttaggregates")

// Function to upsert `microBatchOutputDF` into Delta table using MERGE
def upsertToDelta(microBatchOutputDF: DataFrame, batchId: Long) {
  // ===================================================
  // For DBR 6.0 and above, you can use Merge Scala APIs
  // ===================================================
  deltaTable.as("t")
    .merge(
      microBatchOutputDF.as("s"), 
      "s.key = t.key")
    .whenMatched().updateAll()
    .whenNotMatched().insertAll()
    .execute()
}

spark.conf.set("spark.sql.shuffle.partitions", "1")

// Define the aggregation
val aggregatesDF = spark.readStream
  .format("rate")
  .option("rowsPerSecond", "1000")
  .load()
  .select('value % 100 as("key"))
  .groupBy("key")
  .count()
println("hello Im here")
aggregatesDF.writeStream
  .format("delta")
  .foreachBatch(upsertToDelta _)
  .outputMode("update")
  .start()

// COMMAND ----------

// MAGIC %sql select * from ttaggregates

// COMMAND ----------

Seq((10L,111L)).toDF("key", "count").write
  .format("delta").mode("overwrite").saveAsTable("ttaggregates")

// COMMAND ----------

Seq((2500l,20l)).toDF("count","key").write
  .format("delta").mode("append").saveAsTable("ttaggregates")

// COMMAND ----------

Seq(25000010l).toDF("count_new2").write
  .format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("ttaggregates")

// COMMAND ----------

// MAGIC %sql select * from  ttaggregates

// COMMAND ----------

val events = spark.read.json("/databricks-datasets/structured-streaming/events/")

// COMMAND ----------

events.printSchema

// COMMAND ----------

events.write.partitionBy("date").format("delta").save("/mnt/deltabirtest/events")

// COMMAND ----------

describe history ttaggregates

// COMMAND ----------

// MAGIC %scala dbutils.notebook.getContext().apiUrl.getOrElse("chillei")

// COMMAND ----------

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// Convenience function for turning JSON strings into DataFrames.
def jsonToDataFrame(json: String, schema: StructType = null): DataFrame = {
  // SparkSessions are available with Spark 2.0+
  //val reader = spark.read
  //Option(schema).foreach(reader.schema)
  spark.read.json(sc.parallelize(Array(json)))
}

val schema = new StructType().add("a", new StructType().add("b", IntegerType))
                          
val events = jsonToDataFrame("""
{
  "a": {
     "b": 1,
     "c": 2
  }
}
""", schema)

display(events.select("a.*"))

// COMMAND ----------

val reader = spark.read
reader.json
/* comment 1 */

// COMMAND ----------

events.printSchema

// COMMAND ----------

val xx=spark.read

// COMMAND ----------

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
val inputPath = "/databricks-datasets/structured-streaming/events/"

// Since we know the data format already, let's define the schema to speed up processing (no need for Spark to infer schema)
val jsonSchema = new StructType().add("time", TimestampType).add("action", StringType)

val staticInputDF = 
  spark
    .read
    .schema(jsonSchema)
    .json(inputPath)

display(staticInputDF)

// COMMAND ----------

staticInputDF.printSchema

// COMMAND ----------

import org.apache.spark.sql.functions._

val staticCountsDF = 
  staticInputDF
    .groupBy($"action", window($"time", "1 hour"))
    .count()   

// Register the DataFrame as table 'static_counts'
staticCountsDF.createOrReplaceTempView("static_counts")

// COMMAND ----------

// MAGIC %sql select * from static_counts

// COMMAND ----------

import org.apache.spark.sql.functions._

// Similar to definition of staticInputDF above, just using `readStream` instead of `read`
val streamingInputDF = 
  spark
    .readStream                       // `readStream` instead of `read` for creating streaming DataFrame
    .schema(jsonSchema)               // Set the schema of the JSON data
    .option("maxFilesPerTrigger", 1)  // Treat a sequence of files as a stream by picking one file at a time
.option("startingOffsets", "earliest")  
    .json(inputPath)

// Same query as staticInputDF
val streamingCountsDF = 
  streamingInputDF
    .groupBy($"action", window($"time", "1 hour"))
    .count()

// Is this DF actually a streaming DF?
streamingCountsDF.isStreaming

// COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "1")  // keep the size of shuffles small
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.functions._
val query =
  streamingCountsDF
.writeStream
    .format("delta")
    .option("path", "/tmp/birstreaming/data")
    .option("checkpointLocation", "/tmp/birstreaming/checkpointdir")
    .partitionBy("action")
  .start()

// COMMAND ----------

// MAGIC %fs rm -r /tmp/birstreaming/checkpointdir/

// COMMAND ----------

import org.apache.spark.sql.functions._

// Similar to definition of staticInputDF above, just using `readStream` instead of `read`
val streamingInputDF = 
  spark
    .readStream                       // `readStream` instead of `read` for creating streaming DataFrame
    .schema(jsonSchema)               // Set the schema of the JSON data
    .option("maxFilesPerTrigger", 100)  // Treat a sequence of files as a stream by picking one file at a time
.option("startingOffsets", "earliest")  
    .json(inputPath)

// Is this DF actually a streaming DF?
streamingCountsDF.isStreaming

// COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "1")  // keep the size of shuffles small
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.functions._
val query =
  streamingInputDF
  .writeStream
.format("delta")
    .option("path", "/tmp/birstreaming/data")
    .option("checkpointLocation", "/tmp/birstreaming/checkpointdir")

  .start()

// COMMAND ----------

// MAGIC %sql select count(*) from birdeltajob

// COMMAND ----------

// MAGIC %sql create table birdeltatriggeronce using delta location '/tmp/birstreaming/datajobtriggeronce'

// COMMAND ----------

// MAGIC %sql select count(*) from birdeltatriggeronce

// COMMAND ----------

