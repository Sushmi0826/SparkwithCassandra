package com.scalaspark.exercises

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}


object SparkpocJoins  {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Sparkpoc")
      .master("local[3]")
      .config("spark.cassandra.connection.host", "localhost")
      .config("spark.cassandra.connection.port", "9042")
      .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions")
      .config("spark.sql.catalog.lh", "com.datastax.spark.connector.datasource.CassandraCatalog")
      .getOrCreate()
    //Finding deactivated
    /*var weekdf = spark.read.option("header", true).csv("Inputdata/week_poc.csv")
    var monthdf = spark.read.option("header", true).csv("Inputdata/Monthly_deac.csv")
    val joinedDF= weekdf.join(monthdf,Seq("npi"),joinType = "left_semi")
    joinedDF.show()
    joinedDF.write
      .format("org.apache.spark.sql.cassandra")
      .option("keyspace", "prac")
      .option("table", "deadop")
      .mode("append")
      .save()*/
    //Finding activated
    var weekdf = spark.read.option("header", true).csv("Inputdata/week_poc.csv")
    var monthdf = spark.read.option("header", true).csv("Inputdata/Monthly_deac.csv")
    val joinedDF = weekdf.join(monthdf, Seq("npi"), joinType = "left_anti")
    joinedDF.show()
    joinedDF.write
      .format("org.apache.spark.sql.cassandra")
      .option("keyspace", "prac")
      .option("table", "actdop")
      .mode("append")
      .save()
  }

}
