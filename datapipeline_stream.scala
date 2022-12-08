package ks1pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame

object obj2 {
  
  def main(args:Array[String]):Unit={
    
    val conf = new SparkConf().setAppName("structstream1").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits
    
    val streamstruct1=StructType(
Array(
StructField("nationality", StringType, true),
StructField("seed", StringType, true),
StructField("version", StringType, true),
StructField(

"results", ArrayType(StructType(Array(
StructField("user",new StructType().add("gender", StringType)
.add("name",new StructType().add("title", StringType)
.add("first", StringType)
.add("last", StringType)).add("location",new StructType().add("street", StringType)
.add("city", StringType)
.add("state", StringType)
.add("zip", StringType))
.add("email", StringType)
.add("username", StringType)
.add("password", StringType)
.add("salt", StringType)
.add("md5", StringType)
.add("sha1", StringType)
.add("sha256", StringType)
.add("registered", StringType)
.add("dob", StringType)
.add("phone", StringType)
.add("cell", StringType)
.add("NINO", StringType)
.add("picture", new StructType().add("large", StringType)
.add("medium", StringType)
.add("thumbnail", StringType)))))))))

    
    
    val kafkadf = spark.readStream.format("kafka")
                  .option("kafka.bootstrap.servers","localhost:9092")
                  .option("subscribe","ztp")
                  .load().select("value")
                  .withColumn("value",expr("CAST(value as STRING)"))
                  .withColumn("jsondata",from_json(col("value"),streamstruct1))
                         .select("jsondata.*")
                         kafkadf.printSchema()
                         
kafkadf.withColumn("results",explode(col("results")))
		.select("nationality",
				"seed","version","results.user.username","results.user.cell",
				"results.user.dob","results.user.email"
				,"results.user.gender","results.user.location.city",
				"results.user.location.state","results.user.location.street","results.user.location.zip",
				"results.user.md5","results.user.name.first","results.user.name.last","results.user.name.title","results.user.password",
				"results.user.phone","results.user.picture.large",
				"results.user.picture.medium","results.user.picture.thumbnail",
				"results.user.registered","results.user.salt","results.user.sha1","results.user.sha256")
		.withColumn("username",regexp_replace(col("username"),  "([0-9])", ""))
		
		.writeStream.foreachBatch{ (df: DataFrame, id:Long)=>
		df.write.format("org.apache.spark.sql.cassandra").mode("overwrite")
		.option("spark.cassandra.connection.host","localhost")
		.option("spark.cassandra.connection.port","9042")
		.option("keyspace","apistream1")
		.option("table","apitab1")
		.save()
		
		
    }
		.option("checkpointLocation", "file:///D:/output/structcheck1")
		.start().awaitTermination()
              
                        
      
  }
  
}
