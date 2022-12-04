package pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._

object obj3 {
  
  def main(args:Array[String]):Unit={
    
    val conf = new SparkConf().setAppName("window1").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits
    
    println    
    println("============ Raw Table 1 =============")
    println
    
    val df1old = spark.read.format("csv")
                   .option("header","true")
                   .option("delimiter","~")
                   .load("file:///D:/Divyansh/Work/DataSet/window func scenario/table1.txt")
        df1old.show()

    val df1 = df1old.withColumn("Attribute_name",expr("split(Attribute_name,',')"))
                    .withColumn("Attribute_name",explode(col("Attribute_name")))
//          df1.show(false)
//          df1.printSchema()
    println    
    println("============ Raw Table 2 =============")
    println      
    val df2 = spark.read.format("csv")
                   .option("header","true")
                   .option("delimiter","~")
                   .load("file:///D:/Divyansh/Work/DataSet/window func scenario/table2.txt")
                   
    df2.show(false)
    
    val injoin = df1.join(df2,df1("Attribute_name")===df2("Attribute_name"),"inner")
//    injoin.show()
//    injoin.printSchema()
    
    val win = Window.partitionBy("Order no").orderBy("priority")
    val rankdf = injoin.withColumn("ranked",dense_rank().over(win)) //.orderBy(col("ranked"))
//    rankdf.show()        
//    rankdf.printSchema()
    println    
    println("============ Result =============")
    println
    val finaldf = rankdf.filter(col("ranked")===1).orderBy(col("Order no"))
                        .select("Order no","theme")
                        
          finaldf.show()
  }
  
  
}
