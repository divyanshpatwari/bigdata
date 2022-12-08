/* ================================ OUTPUT AT THE END BELOW ==================================  */
package complexjson1pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import scala.io.Source

object complexjson1 {
  
  def main(args:Array[String]):Unit={
    
    val conf = new SparkConf().setAppName("xml1").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits
    
    val data = Source.fromURL("<URL_data_link>")
    val datastr = data.mkString
    
    val df = spark.read.json(sc.parallelize(List(datastr)))

   
     val flatdf = df.withColumn("data",explode(col("data")))
                    .withColumn("data",explode(col("data")))
                    .withColumn("approvals_explode",explode(col("meta.view.approvals")))
                    .withColumn("columns_explode",explode(col("meta.view.columns")))

     val finaldf = flatdf.select(
                               col("meta.view.oid"),
                               col("meta.view.owner.id"),
                               col("meta.view.owner.displayName"),
                               col("columns_explode.description"),
                               col("approvals_explode.state")
                                 )              
     .na.fill(0)
     .na.fill("NOT AVAILABLE")
                    
     finaldf.show()
     finaldf.printSchema()
 }
}

/*
========================== BEFORE =====================================
+--------------------+--------------------+
|                data|                meta|
+--------------------+--------------------+
|[[row-thej~feqf.i...|[[[[1559931329, t...|
+--------------------+--------------------+
root
 |-- data: array (nullable = true)
 |    |-- element: array (containsNull = true)
 |    |    |-- element: string (containsNull = true)
 |-- meta: struct (nullable = true)
 |    |-- view: struct (nullable = true)
 |    |    |-- approvals: array (nullable = true)
 |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |-- reviewedAt: long (nullable = true)
 |    |    |    |    |-- reviewedAutomatically: boolean (nullable = true)
 |    |    |    |    |-- state: string (nullable = true)
 |    |    |    |    |-- submissionDetails: struct (nullable = true)
 |    |    |    |    |    |-- permissionType: string (nullable = true)
 |    |    |    |    |-- submissionId: long (nullable = true)
 |    |    |    |    |-- submissionObject: string (nullable = true)
 |    |    |    |    |-- submissionOutcome: string (nullable = true)
 |    |    |    |    |-- submissionOutcomeApplication: struct (nullable = true)
 |    |    |    |    |    |-- failureCount: long (nullable = true)
 |    |    |    |    |    |-- status: string (nullable = true)
 |    |    |    |    |-- submittedAt: long (nullable = true)
 |    |    |    |    |-- submitter: struct (nullable = true)
 |    |    |    |    |    |-- displayName: string (nullable = true)
 |    |    |    |    |    |-- id: string (nullable = true)
 |    |    |    |    |-- workflowId: long (nullable = true)
 |    |    |-- assetType: string (nullable = true)
 |    |    |-- attribution: string (nullable = true)
 |    |    |-- averageRating: long (nullable = true)
 |    |    |-- category: string (nullable = true)
 |    |    |-- clientContext: struct (nullable = true)
 |    |    |    |-- clientContextVariables: array (nullable = true)
 |    |    |    |    |-- element: string (containsNull = true)
 |    |    |-- columns: array (nullable = true)
 |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |-- computationStrategy: struct (nullable = true)
 |    |    |    |    |    |-- parameters: struct (nullable = true)
 |    |    |    |    |    |    |-- primary_key: string (nullable = true)
 |    |    |    |    |    |    |-- region: string (nullable = true)
 |    |    |    |    |    |-- source_columns: array (nullable = true)
 |    |    |    |    |    |    |-- element: string (containsNull = true)
 |    |    |    |    |    |-- type: string (nullable = true)
 |    |    |    |    |-- dataTypeName: string (nullable = true)
 |    |    |    |    |-- description: string (nullable = true)
 |    |    |    |    |-- fieldName: string (nullable = true)
 |    |    |    |    |-- flags: array (nullable = true)
 |    |    |    |    |    |-- element: string (containsNull = true)
 |    |    |    |    |-- format: struct (nullable = true)
 |    |    |    |    |    |-- align: string (nullable = true)
 |    |    |    |    |-- id: long (nullable = true)
 |    |    |    |    |-- name: string (nullable = true)
 |    |    |    |    |-- position: long (nullable = true)
 |    |    |    |    |-- renderTypeName: string (nullable = true)
 |    |    |    |    |-- tableColumnId: long (nullable = true)
 |    |    |-- createdAt: long (nullable = true)
 |    |    |-- description: string (nullable = true)
 |    |    |-- displayType: string (nullable = true)
 |    |    |-- downloadCount: long (nullable = true)
 |    |    |-- flags: array (nullable = true)
 |    |    |    |-- element: string (containsNull = true)
 |    |    |-- grants: array (nullable = true)
 |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |-- flags: array (nullable = true)
 |    |    |    |    |    |-- element: string (containsNull = true)
 |    |    |    |    |-- inherited: boolean (nullable = true)
 |    |    |    |    |-- type: string (nullable = true)
 |    |    |-- hideFromCatalog: boolean (nullable = true)
 |    |    |-- hideFromDataJson: boolean (nullable = true)
 |    |    |-- id: string (nullable = true)
 |    |    |-- metadata: struct (nullable = true)
 |    |    |    |-- availableDisplayTypes: array (nullable = true)
 |    |    |    |    |-- element: string (containsNull = true)
 |    |    |    |-- custom_fields: struct (nullable = true)
 |    |    |    |    |-- Identification: struct (nullable = true)
 |    |    |    |    |    |-- Metadata Language: string (nullable = true)
 |    |    |    |    |    |-- Originator: string (nullable = true)
 |    |    |    |    |-- Notes: struct (nullable = true)
 |    |    |    |    |    |-- 1. : string (nullable = true)
 |    |    |    |    |    |-- 2. : string (nullable = true)
 |    |    |    |    |    |-- 3. : string (nullable = true)
 |    |    |    |    |    |-- 4. : string (nullable = true)
 |    |    |    |    |    |-- 5. : string (nullable = true)
 |    |    |    |    |    |-- 6. : string (nullable = true)
 |    |    |    |    |-- Temporal: struct (nullable = true)
 |    |    |    |    |    |-- Period of Time: string (nullable = true)
 |    |    |    |    |    |-- Posting Frequency: string (nullable = true)
 |    |    |    |-- rowLabel: string (nullable = true)
 |    |    |-- name: string (nullable = true)
 |    |    |-- newBackend: boolean (nullable = true)
 |    |    |-- numberOfComments: long (nullable = true)
 |    |    |-- oid: long (nullable = true)
 |    |    |-- owner: struct (nullable = true)
 |    |    |    |-- displayName: string (nullable = true)
 |    |    |    |-- flags: array (nullable = true)
 |    |    |    |    |-- element: string (containsNull = true)
 |    |    |    |-- id: string (nullable = true)
 |    |    |    |-- profileImageUrlLarge: string (nullable = true)
 |    |    |    |-- profileImageUrlMedium: string (nullable = true)
 |    |    |    |-- profileImageUrlSmall: string (nullable = true)
 |    |    |    |-- screenName: string (nullable = true)
 |    |    |    |-- type: string (nullable = true)
 |    |    |-- provenance: string (nullable = true)
 |    |    |-- publicationAppendEnabled: boolean (nullable = true)
 |    |    |-- publicationDate: long (nullable = true)
 |    |    |-- publicationGroup: long (nullable = true)
 |    |    |-- publicationStage: string (nullable = true)
 |    |    |-- rights: array (nullable = true)
 |    |    |    |-- element: string (containsNull = true)
 |    |    |-- rowsUpdatedAt: long (nullable = true)
 |    |    |-- rowsUpdatedBy: string (nullable = true)
 |    |    |-- tableAuthor: struct (nullable = true)
 |    |    |    |-- displayName: string (nullable = true)
 |    |    |    |-- flags: array (nullable = true)
 |    |    |    |    |-- element: string (containsNull = true)
 |    |    |    |-- id: string (nullable = true)
 |    |    |    |-- profileImageUrlLarge: string (nullable = true)
 |    |    |    |-- profileImageUrlMedium: string (nullable = true)
 |    |    |    |-- profileImageUrlSmall: string (nullable = true)
 |    |    |    |-- screenName: string (nullable = true)
 |    |    |    |-- type: string (nullable = true)
 |    |    |-- tableId: long (nullable = true)
 |    |    |-- tags: array (nullable = true)
 |    |    |    |-- element: string (containsNull = true)
 |    |    |-- totalTimesRated: long (nullable = true)
 |    |    |-- viewCount: long (nullable = true)
 |    |    |-- viewLastModified: long (nullable = true)
 |    |    |-- viewType: string (nullable = true)
 
 ===================== AFTER ===============================
 
|38591322|eagg-6py7|Department of Lic...|The manufacturer ...|approved|
|38591322|eagg-6py7|Department of Lic...|The model of the ...|approved|
|38591322|eagg-6py7|Department of Lic...|This distinguishe...|approved|
|38591322|eagg-6py7|Department of Lic...|This categorizes ...|approved|
|38591322|eagg-6py7|Department of Lic...|Describes how far...|approved|
|38591322|eagg-6py7|Department of Lic...|This is the lowes...|approved|
+--------+---------+--------------------+--------------------+--------+
only showing top 20 rows

root
 |-- oid: long (nullable = false)
 |-- id: string (nullable = false)
 |-- displayName: string (nullable = false)
 |-- description: string (nullable = false)
 |-- state: string (nullable = false)
*/
