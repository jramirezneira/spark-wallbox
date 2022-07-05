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




    //Load charger_log_status data source
    val df_charger_log_status=spark.read.option("encoding", "UTF-8").json("src/main/resources/charger_log_status2/")
      .withColumn("charger_timestamp", from_unixtime(col("charger_timestamp")))

      //.withColumn("session_id",(col("id"))).drop(col("id"))
      .withColumn("charger_id_table_log",(col("charger_id"))).drop(col("charger_id"))
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



    //Load charger_log_session2 data source
    val df_charger_log_session=spark.read.json("src/main/resources/charger_log_session2/")
     //.withColumn("start_time",  col("start_time"))
      .withColumn("charger_id_table_session",(col("charger_id"))).drop(col("charger_id")) //Change column charger_id name to charger_id_table_session
    //  .withColumn("end_time",col("end_time"))
      .withColumn("DiffInSeconds",col("end_time") - col("start_time"))
      .withColumn("DiffInMinutes",round(col("DiffInSeconds")/60))
      .withColumn("DiffInHours",round(col("DiffInSeconds")/3600))
      .withColumn("start_time", from_unixtime(col("start_time"))) //Change type start_time
      .withColumn("end_time", from_unixtime(col("end_time"))) //Change type end_time
      .withColumn("session_id",(col("id"))).drop(col("id")) //Change name id to session_id
      .orderBy(col("DiffInSeconds").desc)
    //df_charger_log_session.printSchema()
    //df_charger_log_session.show(false)
    df_charger_log_session.show()
    println(df_charger_log_session.count())
    df_charger_log_session.limit(1000).write.mode(SaveMode.Overwrite).jdbc(url, table="log_session_time",  connectionProperties)

    df_charger_log_session.groupBy("charger_id_table_session")
      .agg(
        min("DiffInSeconds").alias("min_DiffInSeconds"),
        avg("DiffInSeconds").alias("avg_DiffInSeconds"),
        max("DiffInSeconds").alias("max_DiffInSeconds")

      )
      .limit(1000).write.mode(SaveMode.Overwrite).jdbc(url, table="charger_time_aggregations",  connectionProperties)



  // join tables df_charger_log_status df_charger_log_session
    val df_charger = df_charger_log_session.join(
                                df_charger_log_status,
                                df_charger_log_session("charger_id_table_session") ===  df_charger_log_status("charger_id_table_log") &&
                                df_charger_log_session("start_time") <=  df_charger_log_status("charger_timestamp") &&
                                df_charger_log_session("end_time") >=  df_charger_log_status("charger_timestamp"),"inner")

    var df_charger_aggregations = df_charger

    //Create aggragations for temperature, ac_current_rms and ac_voltage_rms
    df_charger_aggregations.groupBy("charger_id_table_log","session_id")
      .agg(
        min("temperature1").alias("min_temperature_phase1"),
        avg("temperature1").alias("avg_temperature_phase1"),
        max("temperature1").alias("max_temperature_phase1"),
        min("temperature2").alias("min_temperature_phase2"),
        avg("temperature2").alias("avg_temperature_phase2"),
        max("temperature2").alias("max_temperature_phase2"),
        min("temperature3").alias("min_temperature_phase3"),
        avg("temperature3").alias("avg_temperature_phase3"),
        max("temperature3").alias("max_temperature_phase3"),
        min("ac_current_rms1").alias("min_ac_current_rms_phase1"),
        avg("ac_current_rms2").alias("avg_ac_current_rms_phase2"),
        max("ac_current_rms3").alias("max_ac_current_rms_phase3"),
        min("ac_voltage_rms1").alias("min_ac_voltage_rms_phase1"),
        avg("ac_voltage_rms2").alias("avg_ac_voltage_rms_phase2"),
        max("ac_voltage_rms1").alias("max_ac_voltage_rms_phase3")
      )
   .limit(1000).write.mode(SaveMode.Overwrite).jdbc(url, table="charger_aggregations",  connectionProperties) //Store in database
   //df_charger_aggregations.show()

    //Calculate big variations
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
    .limit(1000).write.mode(SaveMode.Overwrite).jdbc(url, table="charger_big_changes",  connectionProperties)

    df_charger.show()


  }
}
