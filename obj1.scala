package ks1pack

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming._
import org.apache.spark.sql.functions
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.sql.functions._

object obj1 {

  def main(args:Array[String]):Unit={
    
    val conf = new SparkConf().setAppName("kafkatest1").setMaster("local[*]")
                              .set("spark.driver.allowMultipleContexts","true")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    val ssc = new StreamingContext(conf,Seconds(2))
    
    val topics = Array("newtp")
    
    val kafkaParams = Map[String, Object]("bootstrap.servers" -> "localhost:9092",
				              "key.deserializer" -> classOf[StringDeserializer],
					              "value.deserializer" -> classOf[StringDeserializer],
					            "group.id" -> "example",
					              "auto.offset.reset" -> "latest"
					             )
					  
					  
				val stream = KafkaUtils.createDirectStream[String, String](
				    ssc,PreferConsistent,Subscribe[String, String](
				        topics, 
				           kafkaParams)).map( x => x.value())
				           
									
									
				           
				 stream.foreachRDD(x=>
				 
				   if(!x.isEmpty()){
				     
				      val df = x.toDF("name").withColumn("time",current_timestamp())
				     
				     df.show(false)
				     df.write.format("jdbc")
				     .mode("append")
				     .option("url","jdbc:mysql://localhost:3306/streamtest1")
				     .option("driver","com.mysql.cj.jdbc.Driver")
				     .option("dbtable","tab1")
				     .option("username","root")
				     .option("password","seagull")
				     .save()
				     
				     
				   }
				 )
				 
				 
				 
				 ssc.start()
				 ssc.awaitTermination()
									
									
									
    
  }
  
}
