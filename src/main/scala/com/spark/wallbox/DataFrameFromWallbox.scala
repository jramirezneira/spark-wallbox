package com.spark.wallbox

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import java.util.Properties
import org.apache.spark.sql.SaveMode


object DataFrameFromWallbox {

  val connectionProperties = new Properties()

  connectionProperties.put("user", "wodjrrhvkhjolj")
  connectionProperties.put("password", "03b048a66da39d245d693a9d7f45d161f19e2c0387c74491bd70e39e1eda6584")

  val driverClass = "org.postgresql.Driver"
  connectionProperties.setProperty("Driver", driverClass)

  val url ="jdbc:postgresql://ec2-54-247-137-184.eu-west-1.compute.amazonaws.com/dc5cdu9d8513qn"

  def main(args:Array[String]):Unit= {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExample")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")





    val df_charger_log_status=spark.read.option("encoding", "UTF-8").json("src/main/resources/charger_log_status2/")
      .withColumn("charger_timestamp", from_unixtime(col("charger_timestamp")))

      //.withColumn("session_id",(col("id"))).drop(col("id"))
      .withColumn("charger_id_status_log",(col("charger_id"))).drop(col("charger_id"))
      .withColumn("phase1",element_at(col("charger_phases"),1))
      .withColumn("phase2",element_at(col("charger_phases"),2))
      .withColumn("phase3",element_at(col("charger_phases"),3)).drop("charger_phases")
      .select("phase1.ac_current_rms", "phase1.ac_voltage_rms", "phase1.temperature", "*").drop("phase1")
      .withColumn("ac_current_rms1",col("ac_current_rms")).drop("ac_current_rms")
      .withColumn("ac_voltage_rms1",col("ac_voltage_rms")).drop("ac_voltage_rms")
      .withColumn("temperature1",col("temperature")).drop("temperature")
      .select("phase2.ac_current_rms", "phase2.ac_voltage_rms", "phase2.temperature", "*").drop("phase2")
      .withColumn("ac_current_rms2",col("ac_current_rms")).drop("ac_current_rms")
      .withColumn("ac_voltage_rms2",col("ac_voltage_rms")).drop("ac_voltage_rms")
      .withColumn("temperature2",col("temperature")).drop("temperature")
      .select("phase3.ac_current_rms", "phase3.ac_voltage_rms", "phase3.temperature", "*").drop("phase3")
      .withColumn("ac_current_rms3",col("ac_current_rms")).drop("ac_current_rms")
      .withColumn("ac_voltage_rms3",col("ac_voltage_rms")).drop("ac_voltage_rms")
      .withColumn("temperature3",col("temperature")).drop("temperature")




     // .withColumn("phase2",array_position(col("charger_phases"),1))
     // .withColumn("phase3",array_position(col("charger_phases"),2))
     // .show()
     // .select(col("uri").alias("url"), col("version"))


    //.withColumn("charger_phases",explode(col("charger_phases"))).select("charger_phases.ac_current_rms","charger_phases.ac_voltage_rms", "charger_phases.temperature", "*")
    // .where(col("charger_id_status_log") === "62339")
   // df_charger_log_status.printSchema()
    //df_charger_log_status.show()


    //df_charger_log_status.printSchema()
    //df_charger_log_status.show(false)

    /*df_charger_log_status.groupBy("charger_id")
      .agg(
        min("temperature").as("min_temperature"),
        avg("temperature").as("avg_temperature"),
        max("temperature").as("max_temperature"),
        min("ac_current_rms").as("min_ac_current_rms"),
        avg("ac_current_rms").as("avg_ac_current_rms"),
        max("ac_current_rms").as("max_ac_current_rms"),
        min("ac_voltage_rms").as("min_ac_voltage_rms"),
        avg("ac_voltage_rms").as("avg_ac_voltage_rms"),
        max("ac_voltage_rms").as("max_ac_voltage_rms")
      )
      .show()*/


    val df_charger_log_session=spark.read.json("src/main/resources/charger_log_session2/")
     .withColumn("start_time",  col("start_time"))
      .withColumn("end_time",col("end_time"))
      .withColumn("DiffInSeconds",col("end_time") - col("start_time"))
      .withColumn("DiffInMinutes",round(col("DiffInSeconds")/60))
      .withColumn("DiffInHours",round(col("DiffInSeconds")/3600))
      .withColumn("start_time", from_unixtime(col("start_time")))
      .withColumn("end_time", from_unixtime(col("end_time")))
    //  .where(col("charger_id") === "62339")//.orderBy(col("charger_id"))
      .withColumn("charger_id_session_log",(col("charger_id"))).drop(col("charger_id"))
      .withColumn("session_id",(col("id"))).drop(col("id"))
      .orderBy(col("DiffInSeconds").desc)
    //df_charger_log_session.printSchema()
    //df_charger_log_session.show(false)
    df_charger_log_session.show()
    println(df_charger_log_session.count())




  //  df_charger_log_session.limit(1000).write.jdbc()
    df_charger_log_session.limit(1000).write.mode(SaveMode.Overwrite).jdbc(url, table="log_session_time",  connectionProperties)


    val df_charger = df_charger_log_session.join(
                                df_charger_log_status,
                                df_charger_log_session("charger_id_session_log") ===  df_charger_log_status("charger_id_status_log") &&
                                df_charger_log_session("start_time") <=  df_charger_log_status("charger_timestamp") &&
                                df_charger_log_session("end_time") >=  df_charger_log_status("charger_timestamp"),"inner")

    var df_charger_aggregations = df_charger

    df_charger_aggregations.groupBy("charger_id_session_log","session_id")
      .agg(
        min("min_temperature1").as("min_temperature_phase1"),
        avg("avg_temperature1").as("avg_temperature_phase1"),
        max("max_temperature1").as("max_temperature_phase1"),
        min("min_temperature2").as("min_temperature_phase2"),
        avg("avg_temperature2").as("avg_temperature_phase2"),
        max("max_temperature2").as("max_temperature_phase2"),
        min("min_temperature3").as("min_temperature_phase3"),
        avg("avg_temperature3").as("avg_temperature_phase3"),
        max("max_temperature3").as("max_temperature_phase3"),
        min("min_ac_current_rms1").as("min_ac_current_rms_phase1"),
        avg("avg_ac_current_rms2").as("avg_ac_current_rms_phase2"),
        max("max_ac_current_rms3").as("max_ac_current_rms_phase3"),
        min("min_ac_voltage_rms1").as("min_ac_voltage_rms_phase1"),
        avg("avg_ac_voltage_rms2").as("avg_ac_voltage_rms_phase2"),
        max("max_ac_voltage_rms1").as("max_ac_voltage_rms_phase3")
      )
    df_charger_aggregations.limit(1000).write.mode(SaveMode.Overwrite).jdbc(url, table="charger_aggregations",  connectionProperties)


    val window = Window.partitionBy("session_id").orderBy("charger_timestamp")
    df_charger.withColumn("previous_temperature1", lag(col("temperature1"), 1, null).over(window))
      .withColumn("previous_temperature2", lag(col("temperature2"), 1, null).over(window))
      .withColumn("previous_temperature3", lag(col("temperature3"), 1, null).over(window))
      .withColumn("previous_ac_current_rms1", lag(col("ac_current_rms1"), 1, null).over(window))
      .withColumn("previous_ac_current_rms2", lag(col("ac_current_rms2"), 1, null).over(window))
      .withColumn("previous_ac_current_rms3", lag(col("ac_current_rms3"), 1, null).over(window))
      .withColumn("previous_ac_voltage_rms1", lag(col("ac_voltage_rms1"), 1, null).over(window))
      .withColumn("previous_ac_voltage_rms2", lag(col("ac_voltage_rms2"), 1, null).over(window))
      .withColumn("previous_ac_voltage_rms3", lag(col("ac_voltage_rms3"), 1, null).over(window))
      .withColumn("var_temperature1", round((col("temperature1")-col("previous_temperature1"))/col("temperature1"),2)).drop("previous_temperature1")
      .withColumn("var_temperature2", round((col("temperature2")-col("previous_temperature2"))/col("temperature2"),2)).drop("previous_temperature2")
      .withColumn("var_temperature3", round((col("temperature3")-col("previous_temperature3"))/col("temperature3"),2)).drop("previous_temperature3")
      .withColumn("var_ac_current_rms1", round((col("ac_current_rms1")-col("previous_ac_current_rms1"))/col("ac_current_rms1"),2)).drop("previous_ac_current_rms1")
      .withColumn("var_ac_current_rms2", round((col("ac_current_rms2")-col("previous_ac_current_rms2"))/col("ac_current_rms2"),2)).drop("previous_ac_current_rms2")
      .withColumn("var_ac_current_rms3", round((col("ac_current_rms3")-col("previous_ac_current_rms3"))/col("ac_current_rms3"),2)).drop("previous_ac_current_rms3")
      .withColumn("var_ac_voltage_rms1", round((col("ac_voltage_rms1") - col("previous_ac_voltage_rms1"))/col("ac_voltage_rms1"),2)).drop("previous_ac_voltage_rms1")
      .withColumn("var_ac_voltage_rms2", round((col("ac_voltage_rms2") - col("previous_ac_voltage_rms2"))/col("ac_voltage_rms2"),2)).drop("previous_ac_voltage_rms2")
      .withColumn("var_ac_voltage_rms3", round((col("ac_voltage_rms3") - col("previous_ac_voltage_rms3"))/col("ac_voltage_rms3"),2)).drop("previous_ac_voltage_rms3")
      .where(col("var_temperature1")>0.1 ||  col("var_temperature1") < -0.1 || col("var_temperature2")>0.1 ||  col("var_temperature2") < -0.1 || col("var_temperature3")>0.1 ||  col("var_temperature3") < -0.1
       || col("var_ac_current_rms1")>0.1 ||  col("var_ac_current_rms1") < -0.1 || col("var_ac_current_rms2")>0.1 ||  col("var_ac_current_rms2") < -0.1 || col("var_ac_current_rms3")>0.1 ||  col("var_ac_current_rms3") < -0.1
       || col("var_ac_voltage_rms1")>0.1 ||  col("var_ac_voltage_rms1") < -0.1 || col("var_ac_voltage_rms2")>0.1 ||  col("var_ac_voltage_rms2") < -0.1 || col("var_ac_voltage_rms3")>0.1 ||  col("var_ac_voltage_rms3") < -0.1
      )
    //  .where(col("session_id")==="4441352")
    df_charger.limit(1000).write.mode(SaveMode.Overwrite).jdbc(url, table="charger_big_changes",  connectionProperties)

     // .show(500)




   /* df_charger_log_session.withColumn("start_time",  col("start_time"))
      .withColumn("end_time",col("end_time"))
      .withColumn("DiffInSeconds",col("end_time") - col("start_time"))
      .withColumn("DiffInMinutes",round(col("DiffInSeconds")/60))
      .withColumn("DiffInHours",round(col("DiffInSeconds")/3600))
      .show()*/













  /*
    //spark read csv file
    val df = spark.read.csv("src/main/resources/zipcodes.csv")
    df.show()
    df.printSchema()

    //read csv with options
    val df2 = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv("src/main/resources/zipcodes.csv")
    df2.show()
    df2.printSchema()

    //read with custom schema
    import org.apache.spark.sql.types._
    val schema = new StructType()
      .add("RecordNumber",IntegerType,true)
      .add("Zipcode",IntegerType,true)
      .add("ZipCodeType",StringType,true)
      .add("City",StringType,true)
      .add("State",StringType,true)
      .add("LocationType",StringType,true)
      .add("Lat",DoubleType,true)
      .add("Long",DoubleType,true)
      .add("Xaxis",DoubleType,true)
      .add("Yaxis",DoubleType,true)
      .add("Zaxis",DoubleType,true)
      .add("WorldRegion",StringType,true)
      .add("Country",StringType,true)
      .add("LocationText",StringType,true)
      .add("Location",StringType,true)
      .add("Decommisioned",BooleanType,true)
      .add("TaxReturnsFiled",IntegerType,true)
      .add("EstimatedPopulation",IntegerType,true)
      .add("TotalWages",IntegerType,true)
      .add("Notes",StringType,true)

    //Write dataframe back to csv file
    val df_with_schema = spark.read.format("csv")
      .option("header", "true")
      .schema(schema)
      .load("src/main/resources/zipcodes.csv")

    df_with_schema.printSchema()
    df_with_schema.show(false)


    //Write a csv file
    df_with_schema.write.mode(SaveMode.Overwrite)
      .csv("c:/tmp/spark_output/zipcodes")

   */

  }
}
