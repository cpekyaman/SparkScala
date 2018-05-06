package section6

import org.apache.log4j._
import org.apache.spark.sql._

object SparkSQL {
  
  case class Person(ID:Int, name:String, age:Int, numFriends:Int)
  
  def mapper(line:String): Person = {
    val fields = line.split(',')
    Person(fields(0).toInt, fields(1), fields(2).toInt, fields(3).toInt)
  }
  
  /** Our main function where the action happens */
  def main(args: Array[String]) {
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Use new SparkSession interface in Spark 2.0
    // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///E:/DATA/Spark/Temp")
      .getOrCreate()
    
    val people = spark.sparkContext.textFile("./UdemySparkCourse/src/main/resources/data/fakefriends.csv").map(mapper)
    
    // Infer the schema, and register the DataSet as a table.
    import spark.implicits._
    val schemaPeople = people.toDS
    schemaPeople.printSchema()
    schemaPeople.createOrReplaceTempView("people")
    
    // SQL can be run over DataFrames that have been registered as a table
    val teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")
    teenagers.collect().foreach(println)
    
    spark.stop()
  }
}