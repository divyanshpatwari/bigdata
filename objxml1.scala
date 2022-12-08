package pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._

object objxml1 {
  
  def main(args:Array[String]):Unit={
    
    val conf = new SparkConf().setAppName("xml1").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits
    
    val df = spark.read.format("json")
                       .option("multiline","true")
                       .load("file:///D:/Divyansh/Work/DataSet/electric.json")
                       
       df.show()
       df.printSchema()
   
     val flatdf = df.withColumn("data",explode(col("data")))
                    .withColumn("data",explode(col("data")))
                    .withColumn("approvals_explode",explode(col("meta.view.approvals"))).drop(col("meta.view.approvals"))
                    .withColumn("columns_explode",explode(col("meta.view.columns")))
                    
                    
                      .select(
                          col("data"),
                          col("meta.view.*"),
                          col("approvals_explode"),
                          col("columns_explode")
                      )
                    .drop(col("approvals"))
                    .drop(col("columns"))

                       
     
     
     
     
     
     flatdf.show()
     flatdf.printSchema()

 }
}