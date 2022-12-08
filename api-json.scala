/* ============================= OUTPUT AT THE END BELOW ==================================== */
package apisamplepack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.io.Source

object apisampleobj {

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

/*
+-----------+--------------------+------------------+-------+
|nationality|             results|              seed|version|
+-----------+--------------------+------------------+-------+
|         DE|[[[0174-2192983, ...|8548bb3f59f0d89e04|    0.8|
+-----------+--------------------+------------------+-------+

root
 |-- nationality: string (nullable = true)
 |-- results: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- user: struct (nullable = true)
 |    |    |    |-- cell: string (nullable = true)
 |    |    |    |-- dob: long (nullable = true)
 |    |    |    |-- email: string (nullable = true)
 |    |    |    |-- gender: string (nullable = true)
 |    |    |    |-- location: struct (nullable = true)
 |    |    |    |    |-- city: string (nullable = true)
 |    |    |    |    |-- state: string (nullable = true)
 |    |    |    |    |-- street: string (nullable = true)
 |    |    |    |    |-- zip: long (nullable = true)
 |    |    |    |-- md5: string (nullable = true)
 |    |    |    |-- name: struct (nullable = true)
 |    |    |    |    |-- first: string (nullable = true)
 |    |    |    |    |-- last: string (nullable = true)
 |    |    |    |    |-- title: string (nullable = true)
 |    |    |    |-- password: string (nullable = true)
 |    |    |    |-- phone: string (nullable = true)
 |    |    |    |-- picture: struct (nullable = true)
 |    |    |    |    |-- large: string (nullable = true)
 |    |    |    |    |-- medium: string (nullable = true)
 |    |    |    |    |-- thumbnail: string (nullable = true)
 |    |    |    |-- registered: long (nullable = true)
 |    |    |    |-- salt: string (nullable = true)
 |    |    |    |-- sha1: string (nullable = true)
 |    |    |    |-- sha256: string (nullable = true)
 |    |    |    |-- username: string (nullable = true)
 |-- seed: string (nullable = true)
 |-- version: string (nullable = true)

+---+-------------+------+---------------+--------------------+-----+-----+----------+------+------+--------------------+-----+--------------------+--------------------+----------+
| id|     username|amount|             ip|            createdt|value|score|regioncode|status|method|                 key|count|                type|                site|statuscode|
+---+-------------+------+---------------+--------------------+-----+-----+----------+------+------+--------------------+-----+--------------------+--------------------+----------+
|  1|beautifulbear| 01743|  68.688.326.58|13/Jun/2012:10:55...|   10|   55|        45| -0500|   GET|           /products|    0|   /product/product4|Mozilla/4.0 (comp...|       100|
|  2|beautifulbear| 75756|  361.631.17.30|01/Nov/2011:10:53...|   10|   53|        01| -0500|   GET|               /demo|    0|               /demo|Jakarta Commons-H...|       100|
|  3|beautifulbear| 05901|   11.308.46.48|21/Jun/2012:01:12...|   01|   12|        33| -0500|   GET|               /news|    0|    /partners/resell|Mozilla/5.0 (comp...|       100|
|  4|beautifulbear| 24538|  361.631.17.30|01/Nov/2011:13:48...|   13|   48|        30| -0500|   GET|               /demo|    0|                   -|Jakarta Commons-H...|       100|
|  5|beautifulbear| 08116|  322.76.611.36|30/Jun/2012:07:19...|   07|   19|        30| -0500|   GET|               /demo|    0|                   -|Jakarta Commons-H...|       100|
|  6|beautifulbear| 04675|  361.631.17.30|01/Nov/2011:16:19...|   16|   19|        00| -0500|   GET|               /demo|    0|                   -|Jakarta Commons-H...|       100|
|  7|beautifulbear| 06575| 367.34.686.631|01/Jul/2012:00:02...|   00|   02|        00| -0500|   GET|               /demo|    0|                   -|Jakarta Commons-H...|       100|
|  8|beautifulbear| 27382|   88.633.11.47|04/Nov/2011:13:00...|   13|   00|        30| -0500|   GET|        /feeds/press|    0|                   -|Apple-PubSub/65.12.1|       100|
|  9|beautifulbear| 09934|   11.308.46.48|01/Jul/2012:23:13...|   23|   13|        53| -0500|   GET|           /partners|    0|   /product/product2|Mozilla/5.0 (comp...|       100|
| 10|beautifulbear| 01716|683.615.622.618|05/Nov/2011:22:03...|   22|   03|        01| -0500|   GET|    /partners/resell|    0|    /partners/resell|                null|       100|
| 11|beautifulbear| 55195| 682.62.387.672|02/Jul/2012:10:17...|   10|   17|        48| -0500|   GET|/download/downloa...|    0|/download/downloa...|Mozilla/5.0 (Wind...|       100|
| 12|beautifulbear| 69809|  43.68.686.668|09/Nov/2011:05:25...|   05|   25|        00| -0500|   GET|        /feeds/press|    0|                   -|  Apple-PubSub/65.11|       100|
| 13|beautifulbear| 78800|321.336.372.320|06/Jul/2012:01:41...|   01|   41|        00| -0500|   GET|        /feeds/press|    0|                   -|Mozilla/5.0 (Wind...|       100|
| 14|beautifulbear| 58994|  361.631.17.30|16/Nov/2011:00:50...|   00|   50|        00| -0500|   GET|               /demo|    0|               /demo|Jakarta Commons-H...|       100|
| 15|beautifulbear| 94543| 361.638.668.62|13/Jul/2012:23:44...|   23|   44|        30| -0500|   GET|            /ad/save|    0|                   -|Mozilla/5.0 (Twic...|       100|
| 16|beautifulbear| 33695|   325.87.75.36|16/Nov/2011:04:58...|   04|   58|        30| -0500|   GET|               /demo|    0|               /demo|Jakarta Commons-H...|       100|
| 17|beautifulbear| 01755|  18.332.18.672|16/Jul/2012:03:20...|   03|   20|        30| -0500|   GET|        /feeds/press|    0|                   -|Apple-PubSub/65.12.1|       100|
| 18|beautifulbear| 01784|   361.321.73.6|17/Nov/2011:13:08...|   13|   08|        30| -0500|   GET|               /demo|    0|               /demo|Jakarta Commons-H...|       100|
| 19|beautifulbear| 09671|   364.63.61.83|23/Jul/2012:17:00...|   17|   00|        37| -0500|   GET|   /product/product3|    0|      /docs/doc5.pdf|Mozilla/5.0 (X11;...|       100|
| 20|beautifulbear| 04288|    13.53.52.13|26/Nov/2011:19:16...|   19|   16|        00| -0500|   GET|               /demo|    0|               /demo|Jakarta Commons-H...|       100|
+---+-------------+------+---------------+--------------------+-----+-----+----------+------+------+--------------------+-----+--------------------+--------------------+----------+
only showing top 20 rows

+-----------+------------+----------+--------------------+------+--------------------+--------------------+--------------------+-----+--------------------+---------+---------+-----+--------+------------+--------------------+--------------------+--------------------+----------+--------+--------------------+--------------------+----------------+------------------+-------+
|nationality|        cell|       dob|               email|gender|                city|               state|              street|  zip|                 md5|    first|     last|title|password|       phone|               large|              medium|           thumbnail|registered|    salt|                sha1|              sha256|        username|              seed|version|
+-----------+------------+----------+--------------------+------+--------------------+--------------------+--------------------+-----+--------------------+---------+---------+-----+--------+------------+--------------------+--------------------+--------------------+----------+--------+--------------------+--------------------+----------------+------------------+-------+
|         DE|0174-2192983| 698336133|luis.körner@examp...|  male|         alzey-worms|      sachsen-anhalt|5111 danziger straße|27993|c6636ccc03cdfa6ec...|     luis|   körner|   mr|   rocky|0484-7660248|https://randomuse...|https://randomuse...|https://randomuse...|1198184406|zALjS3bW|6f130e22166040336...|37f7661034135c9da...|      bigostrich|8548bb3f59f0d89e04|    0.8|
|         DE|0174-1199272| 555170593|bruno.berndt@exam...|  male|               trier| nordrhein-westfalen|     5712 feldstraße|30182|7672e366a4525156a...|    bruno|   berndt|   mr|  jennie|0327-7563826|https://randomuse...|https://randomuse...|https://randomuse...|1069446507|IySciHVP|bb9021aab605edb9a...|8199a810e644f9f94...|    blackleopard|8548bb3f59f0d89e04|    0.8|
|         DE|0171-9435787|  34540024|tina.kiefer@examp...|female|                cham|           thüringen|      6131 wiesenweg|18637|2c45a989682a47033...|     tina|   kiefer|  mrs|silverad|0753-0585579|https://randomuse...|https://randomuse...|https://randomuse...|1233288964|MYTMfcSC|dc732b489519992e7...|806a30c4aa21acd0b...|      goldenwolf|8548bb3f59f0d89e04|    0.8|
|         DE|0177-1173423|1058211669|hugo.renner@examp...|  male|                daun|mecklenburg-vorpo...|     4156 kirchplatz|22215|030a8fc4908d95f9e...|     hugo|   renner|   mr| trapper|0841-3814216|https://randomuse...|https://randomuse...|https://randomuse...| 980023605|4NYW6KNj|f3f1aa2b18758df7b...|24191803b266a343f...|      lazyrabbit|8548bb3f59f0d89e04|    0.8|
|         DE|0171-8464676| 349088711|sonja.schulz@exam...|female|              speyer|            saarland|        3069 mühlweg|22078|349625bad27b5d3be...|    sonja|   schulz|   ms|preacher|0695-3870164|https://randomuse...|https://randomuse...|https://randomuse...|1401687867|LMxBXYuQ|02a632bc28b9d284a...|582e639a12bc13b1c...|         redfrog|8548bb3f59f0d89e04|    0.8|
|         DE|0175-3635356| 823000885|manuel.ludwig@exa...|  male|mecklenburg-strelitz|              berlin|     1089 am bahnhof|65323|f1c3c0805a59fcd82...|   manuel|   ludwig|   mr| traffic|0000-2892810|https://randomuse...|https://randomuse...|https://randomuse...|1343314555|59N83bdi|f432c4cfdec3e78e9...|050779a3c8a311f22...|       lazymouse|8548bb3f59f0d89e04|    0.8|
|         DE|0170-2982997| 127899536|sascha.günther@ex...|  male|             bottrop|            saarland|   2390 blumenstraße|87098|28e851a79468f716d...|   sascha|  günther|   mr|    shop|0435-1198193|https://randomuse...|https://randomuse...|https://randomuse...|1150794332|1RYB37mB|1795921bfad7edff4...|337501dc2daca4c6f...|       bluekoala|8548bb3f59f0d89e04|    0.8|
|         DE|0179-1244500| 205277834|henrik.keller@exa...|  male|            schwerin|             hamburg|      6696 lindenweg|70520|ea4d5846a71bf4e8e...|   henrik|   keller|   mr|    shui|0465-1895298|https://randomuse...|https://randomuse...|https://randomuse...|1315997168|shtKdwAK|2f61d6f8a4258f8e8...|8e5bcbe16985c6c85...|     organicfrog|8548bb3f59f0d89e04|    0.8|
|         DE|0177-2018389| 875505552|isabella.weiss@ex...|female|          schwandorf|       niedersachsen|    4331 schulstraße|39824|c32a2ae0c3668d2a2...| isabella|    weiss|  mrs|19691969|0049-8196983|https://randomuse...|https://randomuse...|https://randomuse...| 986074678|aMHSVlJi|6be2108f69312a425...|796b5ab87da0a12ed...|       tinysnake|8548bb3f59f0d89e04|    0.8|
|         DE|0172-4007145|1209920687|pascal.zimmer@exa...|  male|           hasbergen|             sachsen|7240 breslauer st...|15360|6b5602219c3fc8798...|   pascal|   zimmer|   mr|  3x7pxr|0011-6283115|https://randomuse...|https://randomuse...|https://randomuse...|1204675618|mc7GfRye|3b1a535c5ee518df4...|44618606ef917fd22...|       silverdog|8548bb3f59f0d89e04|    0.8|
|         DE|0171-3114084|1166758903|annika.wegner@exa...|female|         ganderkesee|              bremen|      4697 lindenweg|46435|f4c6d0b79bdfb615f...|   annika|   wegner| miss|    1026|0969-0882185|https://randomuse...|https://randomuse...|https://randomuse...|1287684577|gpybZgnC|b9ca6857a2210b6ff...|9d6d36a36fbc3b005...|       greenfrog|8548bb3f59f0d89e04|    0.8|
|         DE|0170-8233350| 600666654|elisabeth.wirth@e...|female|             güstrow|       niedersachsen|      8065 meisenweg|27435|865c66f83b5e415fd...|elisabeth|    wirth|   ms| tiffany|0785-8976884|https://randomuse...|https://randomuse...|https://randomuse...|1026001324|CLdzuRd0|a3c220596e810e34c...|6486ff3d4066c9f53...|   purpleladybug|8548bb3f59f0d89e04|    0.8|
|         DE|0170-2591320| 334638700|jasmina.franke@ex...|female| frankenthal (pfalz)|              bremen|   9614 mühlenstraße|99943|895c5c2355d3b3563...|  jasmina|   franke| miss|beautifu|0160-6092756|https://randomuse...|https://randomuse...|https://randomuse...| 917032055|dhAABxJT|76be90762d15a3c42...|077f56877b4f7c48a...|  brownbutterfly|8548bb3f59f0d89e04|    0.8|
|         DE|0173-7087953|1311888258|tara.berger@examp...|female|   darmstadt-dieburg|              bayern|     2128 jahnstraße|30639|5c9cf2fc609cae285...|     tara|   berger|  mrs|klondike|0706-2286484|https://randomuse...|https://randomuse...|https://randomuse...|1264301058|49fF3nXI|3aca76593c432f6c6...|9def88d6482c0cec4...|    redbutterfly|8548bb3f59f0d89e04|    0.8|
|         DE|0175-3317878| 922332943|jenny.arnold@exam...|female|           kitzingen|              bremen|4390 beethovenstraße|86218|45118482ffc6665f5...|    jenny|   arnold|   ms|   trick|0931-0007031|https://randomuse...|https://randomuse...|https://randomuse...|1050629964|YUILp3v7|7734d937c490e5c18...|828c5028cf895cd00...|   yellowladybug|8548bb3f59f0d89e04|    0.8|
|         DE|0175-6205083|1182973276|michelle.jahn@exa...|female|           rosenheim|             hamburg|     9980 poststraße|78081|5f72e740cc32d5b0b...| michelle|     jahn| miss|hardball|0799-9842135|https://randomuse...|https://randomuse...|https://randomuse...|1296613962|pLmE3xA2|e3d88a9037ce5bff1...|36022f768e715056d...|    whitepeacock|8548bb3f59f0d89e04|    0.8|
|         DE|0170-3351258|1376287748|michelle.rudolph@...|female|              barnim| nordrhein-westfalen|  6580 am sportplatz|91485|e241b72b7018e9c02...| michelle|  rudolph| miss|  blues1|0153-3080003|https://randomuse...|https://randomuse...|https://randomuse...| 935837289|IyQ37aCd|aa15b5ca5a6e9cf0b...|026b20aa32a1fb4f4...|beautifulostrich|8548bb3f59f0d89e04|    0.8|
|         DE|0178-7534727| 950449313|tom.brinkmann@exa...|  male|              barnim|      sachsen-anhalt|3339 berliner straße|18888|869215c0f14943f9b...|      tom|brinkmann|   mr|mohammed|0412-5900540|https://randomuse...|https://randomuse...|https://randomuse...| 968949794|Wc7U9S5w|123c960a2405d61b5...|9c114b0ed8f684a13...|       whitebear|8548bb3f59f0d89e04|    0.8|
|         DE|0171-0114491| 379391850|julius.kraft@exam...|  male|neuburg-schrobenh...|            saarland|     4668 lerchenweg|32483|103abad79f54a6d1b...|   julius|    kraft|   mr|  trixie|0973-4328437|https://randomuse...|https://randomuse...|https://randomuse...|1191348063|JWDmtUua|3324e019aa9f5156e...|0c15afc1d00b75f2b...|        lazybird|8548bb3f59f0d89e04|    0.8|
|         DE|0170-3141872|    594229|valentin.büttner@...|  male|         delmenhorst|              berlin|       6930 rosenweg|87216|b2c18e7dd58304134...| valentin|  büttner|   mr|waterfal|0205-5480690|https://randomuse...|https://randomuse...|https://randomuse...| 927474965|MU68TpdF|dc0cc41f20b8f2db0...|e055b525578d88c85...|    heavygorilla|8548bb3f59f0d89e04|    0.8|
+-----------+------------+----------+--------------------+------+--------------------+--------------------+--------------------+-----+--------------------+---------+---------+-----+--------+------------+--------------------+--------------------+--------------------+----------+--------+--------------------+--------------------+----------------+------------------+-------+
only showing top 20 rows

root
 |-- nationality: string (nullable = true)
 |-- cell: string (nullable = true)
 |-- dob: long (nullable = true)
 |-- email: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- city: string (nullable = true)
 |-- state: string (nullable = true)
 |-- street: string (nullable = true)
 |-- zip: long (nullable = true)
 |-- md5: string (nullable = true)
 |-- first: string (nullable = true)
 |-- last: string (nullable = true)
 |-- title: string (nullable = true)
 |-- password: string (nullable = true)
 |-- phone: string (nullable = true)
 |-- large: string (nullable = true)
 |-- medium: string (nullable = true)
 |-- thumbnail: string (nullable = true)
 |-- registered: long (nullable = true)
 |-- salt: string (nullable = true)
 |-- sha1: string (nullable = true)
 |-- sha256: string (nullable = true)
 |-- username: string (nullable = true)
 |-- seed: string (nullable = true)
 |-- version: string (nullable = true)

+---+-------------+------+---------------+--------------------+-----+-----+----------+------+------+--------------------+-----+--------------------+--------------------+----------+-----------+----+----+-----+------+----+-----+------+----+----+-----+----+-----+--------+-----+-----+------+---------+----------+----+----+------+--------+----+-------+
| id|     username|amount|             ip|            createdt|value|score|regioncode|status|method|                 key|count|                type|                site|statuscode|nationality|cell| dob|email|gender|city|state|street| zip| md5|first|last|title|password|phone|large|medium|thumbnail|registered|salt|sha1|sha256|username|seed|version|
+---+-------------+------+---------------+--------------------+-----+-----+----------+------+------+--------------------+-----+--------------------+--------------------+----------+-----------+----+----+-----+------+----+-----+------+----+----+-----+----+-----+--------+-----+-----+------+---------+----------+----+----+------+--------+----+-------+
|  1|beautifulbear| 01743|  68.688.326.58|13/Jun/2012:10:55...|   10|   55|        45| -0500|   GET|           /products|    0|   /product/product4|Mozilla/4.0 (comp...|       100|       null|null|null| null|  null|null| null|  null|null|null| null|null| null|    null| null| null|  null|     null|      null|null|null|  null|    null|null|   null|
|  2|beautifulbear| 75756|  361.631.17.30|01/Nov/2011:10:53...|   10|   53|        01| -0500|   GET|               /demo|    0|               /demo|Jakarta Commons-H...|       100|       null|null|null| null|  null|null| null|  null|null|null| null|null| null|    null| null| null|  null|     null|      null|null|null|  null|    null|null|   null|
|  3|beautifulbear| 05901|   11.308.46.48|21/Jun/2012:01:12...|   01|   12|        33| -0500|   GET|               /news|    0|    /partners/resell|Mozilla/5.0 (comp...|       100|       null|null|null| null|  null|null| null|  null|null|null| null|null| null|    null| null| null|  null|     null|      null|null|null|  null|    null|null|   null|
|  4|beautifulbear| 24538|  361.631.17.30|01/Nov/2011:13:48...|   13|   48|        30| -0500|   GET|               /demo|    0|                   -|Jakarta Commons-H...|       100|       null|null|null| null|  null|null| null|  null|null|null| null|null| null|    null| null| null|  null|     null|      null|null|null|  null|    null|null|   null|
|  5|beautifulbear| 08116|  322.76.611.36|30/Jun/2012:07:19...|   07|   19|        30| -0500|   GET|               /demo|    0|                   -|Jakarta Commons-H...|       100|       null|null|null| null|  null|null| null|  null|null|null| null|null| null|    null| null| null|  null|     null|      null|null|null|  null|    null|null|   null|
|  6|beautifulbear| 04675|  361.631.17.30|01/Nov/2011:16:19...|   16|   19|        00| -0500|   GET|               /demo|    0|                   -|Jakarta Commons-H...|       100|       null|null|null| null|  null|null| null|  null|null|null| null|null| null|    null| null| null|  null|     null|      null|null|null|  null|    null|null|   null|
|  7|beautifulbear| 06575| 367.34.686.631|01/Jul/2012:00:02...|   00|   02|        00| -0500|   GET|               /demo|    0|                   -|Jakarta Commons-H...|       100|       null|null|null| null|  null|null| null|  null|null|null| null|null| null|    null| null| null|  null|     null|      null|null|null|  null|    null|null|   null|
|  8|beautifulbear| 27382|   88.633.11.47|04/Nov/2011:13:00...|   13|   00|        30| -0500|   GET|        /feeds/press|    0|                   -|Apple-PubSub/65.12.1|       100|       null|null|null| null|  null|null| null|  null|null|null| null|null| null|    null| null| null|  null|     null|      null|null|null|  null|    null|null|   null|
|  9|beautifulbear| 09934|   11.308.46.48|01/Jul/2012:23:13...|   23|   13|        53| -0500|   GET|           /partners|    0|   /product/product2|Mozilla/5.0 (comp...|       100|       null|null|null| null|  null|null| null|  null|null|null| null|null| null|    null| null| null|  null|     null|      null|null|null|  null|    null|null|   null|
| 10|beautifulbear| 01716|683.615.622.618|05/Nov/2011:22:03...|   22|   03|        01| -0500|   GET|    /partners/resell|    0|    /partners/resell|                null|       100|       null|null|null| null|  null|null| null|  null|null|null| null|null| null|    null| null| null|  null|     null|      null|null|null|  null|    null|null|   null|
| 11|beautifulbear| 55195| 682.62.387.672|02/Jul/2012:10:17...|   10|   17|        48| -0500|   GET|/download/downloa...|    0|/download/downloa...|Mozilla/5.0 (Wind...|       100|       null|null|null| null|  null|null| null|  null|null|null| null|null| null|    null| null| null|  null|     null|      null|null|null|  null|    null|null|   null|
| 12|beautifulbear| 69809|  43.68.686.668|09/Nov/2011:05:25...|   05|   25|        00| -0500|   GET|        /feeds/press|    0|                   -|  Apple-PubSub/65.11|       100|       null|null|null| null|  null|null| null|  null|null|null| null|null| null|    null| null| null|  null|     null|      null|null|null|  null|    null|null|   null|
| 13|beautifulbear| 78800|321.336.372.320|06/Jul/2012:01:41...|   01|   41|        00| -0500|   GET|        /feeds/press|    0|                   -|Mozilla/5.0 (Wind...|       100|       null|null|null| null|  null|null| null|  null|null|null| null|null| null|    null| null| null|  null|     null|      null|null|null|  null|    null|null|   null|
| 14|beautifulbear| 58994|  361.631.17.30|16/Nov/2011:00:50...|   00|   50|        00| -0500|   GET|               /demo|    0|               /demo|Jakarta Commons-H...|       100|       null|null|null| null|  null|null| null|  null|null|null| null|null| null|    null| null| null|  null|     null|      null|null|null|  null|    null|null|   null|
| 15|beautifulbear| 94543| 361.638.668.62|13/Jul/2012:23:44...|   23|   44|        30| -0500|   GET|            /ad/save|    0|                   -|Mozilla/5.0 (Twic...|       100|       null|null|null| null|  null|null| null|  null|null|null| null|null| null|    null| null| null|  null|     null|      null|null|null|  null|    null|null|   null|
| 16|beautifulbear| 33695|   325.87.75.36|16/Nov/2011:04:58...|   04|   58|        30| -0500|   GET|               /demo|    0|               /demo|Jakarta Commons-H...|       100|       null|null|null| null|  null|null| null|  null|null|null| null|null| null|    null| null| null|  null|     null|      null|null|null|  null|    null|null|   null|
| 17|beautifulbear| 01755|  18.332.18.672|16/Jul/2012:03:20...|   03|   20|        30| -0500|   GET|        /feeds/press|    0|                   -|Apple-PubSub/65.12.1|       100|       null|null|null| null|  null|null| null|  null|null|null| null|null| null|    null| null| null|  null|     null|      null|null|null|  null|    null|null|   null|
| 18|beautifulbear| 01784|   361.321.73.6|17/Nov/2011:13:08...|   13|   08|        30| -0500|   GET|               /demo|    0|               /demo|Jakarta Commons-H...|       100|       null|null|null| null|  null|null| null|  null|null|null| null|null| null|    null| null| null|  null|     null|      null|null|null|  null|    null|null|   null|
| 19|beautifulbear| 09671|   364.63.61.83|23/Jul/2012:17:00...|   17|   00|        37| -0500|   GET|   /product/product3|    0|      /docs/doc5.pdf|Mozilla/5.0 (X11;...|       100|       null|null|null| null|  null|null| null|  null|null|null| null|null| null|    null| null| null|  null|     null|      null|null|null|  null|    null|null|   null|
| 20|beautifulbear| 04288|    13.53.52.13|26/Nov/2011:19:16...|   19|   16|        00| -0500|   GET|               /demo|    0|               /demo|Jakarta Commons-H...|       100|       null|null|null| null|  null|null| null|  null|null|null| null|null| null|    null| null| null|  null|     null|      null|null|null|  null|    null|null|   null|
+---+-------------+------+---------------+--------------------+-----+-----+----------+------+------+--------------------+-----+--------------------+--------------------+----------+-----------+----+----+-----+------+----+-----+------+----+----+-----+----+-----+--------+-----+-----+------+---------+----------+----+----+------+--------+----+-------+
only showing top 20 rows

root
 |-- id: string (nullable = true)
 |-- username: string (nullable = true)
 |-- amount: string (nullable = true)
 |-- ip: string (nullable = true)
 |-- createdt: string (nullable = true)
 |-- value: string (nullable = true)
 |-- score: string (nullable = true)
 |-- regioncode: string (nullable = true)
 |-- status: string (nullable = true)
 |-- method: string (nullable = true)
 |-- key: string (nullable = true)
 |-- count: string (nullable = true)
 |-- type: string (nullable = true)
 |-- site: string (nullable = true)
 |-- statuscode: string (nullable = true)
 |-- nationality: string (nullable = true)
 |-- cell: string (nullable = true)
 |-- dob: long (nullable = true)
 |-- email: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- city: string (nullable = true)
 |-- state: string (nullable = true)
 |-- street: string (nullable = true)
 |-- zip: long (nullable = true)
 |-- md5: string (nullable = true)
 |-- first: string (nullable = true)
 |-- last: string (nullable = true)
 |-- title: string (nullable = true)
 |-- password: string (nullable = true)
 |-- phone: string (nullable = true)
 |-- large: string (nullable = true)
 |-- medium: string (nullable = true)
 |-- thumbnail: string (nullable = true)
 |-- registered: long (nullable = true)
 |-- salt: string (nullable = true)
 |-- sha1: string (nullable = true)
 |-- sha256: string (nullable = true)
 |-- username: string (nullable = true)
 |-- seed: string (nullable = true)
 |-- version: string (nullable = true)
 */
