package project1pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.io.Source

object obj1 {
  
  
  def main(args:Array[String]):Unit={
    
  val conf = new SparkConf().setAppName("project1").setMaster("local[*]")
  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")
  
  val spark = SparkSession.builder().getOrCreate()
  import spark.implicits._
  
  val df1 = spark.read.format("avro")
                      .option("header","true")
                      .load("hdfs://192.168.29.120:8020/user/cloudera/customer_stage_loc/part-m-00000.avro")
                      
  df1.show()
  df1.printSchema()
    
  val read = Source.fromURL("https://randomuser.me/api/0.8/?results=500")
  val apidata = read.mkString
  
  val apirdd = sc.parallelize(List(apidata))
  val apidf = spark.read.json(apirdd)
  
  apidf.show()
  apidf.printSchema()
  
  val apiflat = apidf.withColumn("results",explode(col("results")))
                     .select(
                     col("nationality"),
                     col("results.user.cell"),    
                     col("results.user.dob"),    
                     col("results.user.email"),    
                     col("results.user.gender"),    
                     col("results.user.location.*"),
                     col("results.user.md5"),
                     col("results.user.name.*"),
                     col("results.user.password"),
                     col("results.user.phone"),
                     col("results.user.picture.*"),
                     col("results.user.registered"),
                     col("results.user.salt"),
                     col("results.user.sha1"),
                     col("results.user.sha256"),
                     col("results.user.username"),
                     col("seed"),
                     col("version")
                     )
                     .withColumn("username",regexp_replace(col("username"),"[0-9]",""))
  apiflat.show()
  apiflat.printSchema()
  
  val join = df1.join(broadcast(apiflat),df1("username")===apiflat("username"),"left")
  join.show()
  join.printSchema()
  
  val natnull = join.filter(join("nationality").isNull)
                    .na.fill(0)
                    .na.fill("NOT AVAILABLE")
                    .withColumn("current_date",current_date())
  natnull.show()
  
  val notnull = join.filter(join("nationality").isNotNull)
                    .withColumn("current_date",current_date()) 
  notnull.show()
  }
  
}
