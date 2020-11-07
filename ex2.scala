package org.apache.spark.examples.sql

// $example on:programmatic_schema$
import org.apache.spark.sql.Row
// $example off:programmatic_schema$
// $example on:init_session$
import org.apache.spark.sql.SparkSession
// $example off:init_session$
// $example on:programmatic_schema$
// $example on:data_types$
import org.apache.spark.sql.types._
// $example off:data_types$
// $example off:programmatic_schema$
import org.apache.log4j.{Level, Logger}


object SparkSQLExample {
  Logger.getLogger("org").setLevel(Level.ERROR)
  // $example on:create_ds$
  // case class Person(name: String, age: Long)
  // $example off:create_ds$

  def main(args: Array[String]): Unit = {
    // $example on:init_session$
    val spark = SparkSession
      .builder
      .master("local[2]")
      .appName("Spark Exercise2 HW")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._
    // $example off:init_session$

    //    Read insurance.csv file (uploaded in slack channel week6)
    val df = spark.read.option("header",true).csv("insurance.csv")
    df.show()
    //    Print the size
    println(df.count() + " rows")
    //    Print sex and count of sex (use group by in sql)
    df.createOrReplaceTempView("people")
    val dfSex = spark.sql("SELECT COUNT(sex), sex FROM people GROUP BY sex")
    dfSex.show()
    //    Filter smoker=yes and print again the sex,count of sex
    val dfSexSmoker = spark.sql("SELECT COUNT(sex), sex FROM people WHERE smoker='yes' GROUP BY sex")
    dfSexSmoker.show()
    //    Group by region and sum the charges (in each region), then print rows by descending order (with respect to sum)
    val dfReg = spark.sql("SELECT region, SUM(charges) FROM people GROUP BY region ORDER BY SUM(charges) DESC")
    dfReg.show()

    spark.stop()
  }
}