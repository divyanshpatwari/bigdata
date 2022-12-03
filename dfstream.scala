package ks1pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object obj2 {
  
  def main(args:Array[String]):Unit={
    
    val conf = new SparkConf().setAppName("structstream1").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits
    
    
    val df = spark.readStream.format("kafka")
                  .option("kafka.bootstrap.servers","localhost:9092")
                  .option("subscribe","stk")
                  .load()
                  .withColumn("value",expr("CAST(value as string)"))
                  .select("value")
                  
        df.writeStream.format("console")
          .option("checkpointLocation","file:///D:/output/kdata")
          .start()
          .awaitTermination()
    
  }
  
}
