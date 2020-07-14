package com.spark.sql

import org.apache.spark.sql.{Row, SparkSession}
import Config.getSparkConf
import org.apache.log4j.Logger

import scala.util.Random
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object SparkSqlDemo {
  @transient lazy val logger=Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    logger.info("starts sparksesssion")
    val spark=SparkSession.builder().config(getSparkConf).getOrCreate()
    logger.info("Spark session has been created")

    logger.info("Creating rdd using sparksession of sparkcontext of parallelize method")
    val rdd=spark.sparkContext.parallelize(1 to 10)
    logger.info("Imported scala.util.Random")
    logger.info("Creating a tuple")
//    val rnd =Random.nextInt(100)
//    val mapping=rdd.map(x=>(x,rnd,rnd*x))
    val mapping=rdd.map(x=>(x,Random.nextInt(100)*x))
    logger.info("Importing implicits")
    import spark.implicits._
    logger.info("Creating dataframe from rdd using toDF() implicit method ")
    val df = mapping.toDF("key","value")
//    df.show()
    logger.info("Adding one more column called add to existing dataframe")
    val df1=df.withColumn("add",col("key")+col("value"))
//    df1.show()
    val df2 = df1.withColumn("ConcatTwoCols",concat(col("key"),col("value").cast(StringType)))
//    df2.show()

//====================================================================================//
    val data = Seq(
         Row(Row("James ","","Smith"),"36636","M","3000"),
         Row(Row("Michael ","Rose",""),"40288","M","4000"),
         Row(Row("Robert ","","Williams"),"42114","M","4000"),
         Row(Row("Maria ","Anne","Jones"),"39192","F","4000"),
         Row(Row("Jen","Mary","Brown"),"","F","-1")
         )
    val schema= new StructType().add("name",new StructType().add("fname",StringType).add("mname",StringType).add("lname",StringType))
                .add("dob",StringType).add("sex",StringType).add("sal",StringType)
    val rdd1=spark.sparkContext.parallelize(data)
    val dff=spark.createDataFrame(rdd1,schema)
//    dff.show()
//    dff.show(false)
     val dfschema=dff.withColumn("dob",col("dob").cast(IntegerType))
      .withColumn("salary",col("salary").cast(IntegerType))
       dfschema.printSchema()

    val data1=spark.sparkContext.parallelize(List(("java",100000),("python",50000),("scala",800000)))


    val colums=List("Language","user_count")
    println(colums(0))
    println(colums(1))
    data1.toDF("Language","user_count").printSchema()
    val dfResult=data1.toDF(colums:_*)
    dfResult
// spark.sql("select * from test").show()














  }

}
