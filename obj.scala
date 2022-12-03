package pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.io.Source

object obj {

  def main(args:Array[String]):Unit={
    
    val conf = new SparkConf().setAppName("sparkFirst").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    val data = Source.fromURL("https://randomuser.me/api/0.8/?results=500")
    val datastr = data.mkString
    
    val df1 = spark.read.json(sc.parallelize(List(datastr)))
    
    df1.printSchema()
    
    val df2 = spark.read.format("avro")
                        .option("header","true")
                        .load("s3://divnewbuck/dataset/projectsample.avro")
    df2.show()
                        
    val flattendf1 = df1.withColumn("results",explode(col("results"))) 
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
    flattendf1.show()
    flattendf1.printSchema()
    
    
    val join = df2.join(broadcast(flattendf1),df2("username")===flattendf1("username"),"left")
    join.show()
    join.printSchema()
    
    val nulldata = join.filter(join("nationality").isNull)
                       .na.fill(0)
                       .na.fill("NOT AVAILABLE")
                       .withColumn("current_date",current_date())
    val notnull = join.filter(join("nationality").isNotNull)
                      .withColumn("current_date",current_date())
    
    nulldata.write.format("parquet").mode("overwrite").save("s3://divnewbuck/apidata/availablecustomer")
    notnull.write.format("parquet").mode("overwrite").save("s3://divnewbuck/apidata/notavailable")
                      
                      
  }
  
}