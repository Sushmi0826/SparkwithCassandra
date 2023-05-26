package com.scalaspark.exercises

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}
import org.apache.spark


object Sparkpoc extends Serializable {
  lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Sparkpoc")
      .master("local[3]")
      .getOrCreate()
    val weeklyschemaStruct = StructType(List(
      StructField("NPI", IntegerType),
      StructField("Provider Secondary Practice Location Address- Address Line 1", StringType),
      StructField("Provider Secondary Practice Location Address-  Address Line 2", StringType),
      StructField("Provider Secondary Practice Location Address - City Name", StringType),
      StructField("Provider Secondary Practice Location Address - State Name",StringType),
      StructField("Provider Secondary Practice Location Address - Postal Code",IntegerType),
      StructField("Provider Secondary Practice Location Address - Country Code (If outside U.S.)",StringType),
      StructField("Provider Secondary Practice Location Address - Telephone Extension",IntegerType),
      StructField("Provider Gender Code",StringType)
    ))
    val weeklycsvDF = spark.read.option("header",true).csv("Inputdata/weekly_data.csv")
    val monthlycsvDF = spark.read.option("header",true).csv("Inputdata/Monthly_deac.csv")

    val states = Map(("AK","AK,ALASKA"),("AL","AL,ALABAMA"),("AR","AR,ARKANSAS"),("AS","AS,AMERICAN SAMOA"),("AZ","AZ,ARIZONA"),("CA","CA,CALIFORNIA"),("CO","CO,COLORADO"),
      ("CT","CT,CONNECTICUT"),("DC","DC,DISTRICT OF COLUMBIA"),("DE","DE,DELAWARE"),("FL","FL,FLORIDA"),("FM","FM,MICRONESIA, FEDERATED STATES OF"),("GA","GA,GEORGIA"),
      ("GU","GU,GUAM"),("HI","HI,HAWAII"),("IA","IA,IOWA"),("ID","ID,IDAHO"),("IL","IL,ILLINOIS"),("IN","IN,INDIANA"),("KS","KS,KANSAS"),("KY","KY,KENTUCKY"),("LA","LA,LOUISIANA"),
      ("MA","MA,MASSACHUSETTS"),("MD","MD,MARYLAND"),("ME","ME,MAINE"),("MH","MH,MARSHALL ISLANDS"),("MI","MI,MICHIGAN"),("MN","MN,MINNESOTA"),("MO","MO,MISSOURI"),("MP","MP,MARIANA ISLANDS NORTHERN"),
      ("MS","MS,MISSISSIPPI"),("MT","MT,MONTANA"),("NC","NC,NORTH CAROLINA"),("ND","ND,NORTH DAKOTA"),("NE","NE,NEBRASKA"),("NH","NH,NEW HAMPSHIRE"),("NJ","NJ,NEW JERSEY"),("NM","NM,NEW MEXICO"),
      ("NV","NV,NEVADA"),("NY","NY,NEW YORK"),("OH","OH,OHIO"),("OK","OK,OKLAHOMA"),("OR","OR,OREGON"),("PA","PA,PENNSYLVANIA"),("PR","PR,PUERTO RICO"),("PW","PW,PALAU"),("RI","RI,RHODE ISLAND"),
      ("SC","SC,SOUTH CAROLINA"),("SD","SD,SOUTH DAKOTA"),("TN","TN,TENNESSEE"),("TX","TX,TEXAS"),("UT","UT,UTAH"),("VA","VA,VIRGINIA"),("VI","VI,VIRGIN ISLANDS"),("VT","VT,VERMONT"),
      ("WA","WA,WASHINGTON"),("WI","WI,WISCONSIN"),("WV","WV,WEST VIRGINIA"),("WY","WY,WYOMING"),("ZZ","ZZ,Foreign Country"))
    val countries = Map(("US", "US,United States"))
    val genders = Map(("M","M,Male"),("F","F,Female"))
    val columns = weeklycsvDF.columns
    val cols = monthlycsvDF.columns
    val broadcastStates = spark.sparkContext.broadcast(states)
    val broadcastCountries = spark.sparkContext.broadcast(countries)
    val broadcastGenders = spark.sparkContext.broadcast(genders)
    import spark.implicits._
    val FinalRdd = weeklycsvDF.map(row => {
      val state = row.getString(4)
      val country = row.getString(6)
      val gender = row.getString(8)
      val fullCountry = broadcastCountries.value.get(country)
      val fullState = broadcastStates.value.get(state)
      val fullGender = broadcastGenders.value.get(gender)
      (row.getString(0), row.getString(1), row.getString(2),row.getString(3),fullState,row.getString(5), fullCountry,row.getString(7), fullGender)
    })
    FinalRdd.write.options(Map("header" -> "true", "delimiter" -> ","))
      .csv("Inputdata/Weekly_mod1.csv")
    // .toDF(columns:_*).show(false)
    //println(FinalRdd.collect().mkString("\n"))
    scala.io.StdIn.readLine()
  }
}
