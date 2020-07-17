package com.spark.sql

import org.apache.spark.sql.{Row, SparkSession}
import Config.getSparkConf
import org.apache.log4j.Logger

import scala.util.Random
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.types._

object SparkSqlDemo {
  @transient lazy val logger=Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    logger.info("starts sparksesssion")
    val spark=SparkSession.builder().config(getSparkConf).getOrCreate()
    logger.info("Spark session has been created")

    logger.info("Creating rdd using sparksession of sparkcontext of parallelize method")
    /*val rdd=spark.sparkContext.parallelize(1 to 10)
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
    dfResult*/
// spark.sql("select * from test").show()

    //=====================================2020-07-14===================================//

   /* val rdd =spark.sparkContext.parallelize(1 to 10)
    val mapping=rdd.map{x=>(x,Random.nextInt(100)*x)}
    import spark.implicits._
    val df1=mapping.toDF("key","value")
    df1.show()
    df1.withColumn("add",col("key")+col("value")).show()
    df1.withColumn("ConcatTwoColumn",concat(col("key"), col("value").cast(StringType))).show()
    val data = Seq(
            Row(Row("James ","","Smith"),"36636","M","3000"),
             Row(Row("Michael ","Rose",""),"40288","M","4000"),
             Row(Row("Robert ","","Williams"),"42114","M","4000"),
             Row(Row("Maria ","Anne","Jones"),"39192","F","4000"),
             Row(Row("Jen","Mary","Brown"),"","F","-1")
                   )
    val dataRdd=spark.sparkContext.parallelize(data)
    val schema=new StructType().add("name",new StructType().add("fname",StringType).add("mname",StringType).add("lname",StringType))
                               .add("dob",StringType).add("sex",StringType).add("sal",StringType)

    val df2=spark.createDataFrame(dataRdd,schema)
    df2.show()
    val df3=spark.createDataFrame(dataRdd,schema).show(false)//DOUBT how to create this one by usind toDf//
    df2.printSchema()
    df2.withColumn("sal", col("sal").cast("Integer")).printSchema()
    df2.show()
    df2.withColumn("sal",col("sal")*100).show()
    df2.withColumn("increment",col("sal")*100).show()
    //=======================================================================================//

     df2.withColumn("state",lit("Karnataka")).withColumn("country",lit("India")).show()
     df2.withColumnRenamed("sex","gender").show()
     val df11= df2.withColumn("state",lit("Karnataka")).withColumn("country",lit("India"))
       df11.show()
       df11.drop("state").show()
    val nameAddData = Seq(("Robert, Smith", "1 Main st, Newark, NJ, 92537"),("Maria, Garcia","3456 Walnut st, Newark, NJ, 94732"))
    val columns=Seq("name","address")

   val df4=spark.createDataFrame(nameAddData).toDF(columns:_*)
       df4.printSchema()
       df4.show(false)

    val df5=df4.map(x=>{
      val nameSplit=x.getAs[String](0).split(",")
      val addSplit=x.getAs[String](1).split(",")
      (nameSplit(0),nameSplit(1),addSplit(0),addSplit(1),addSplit(2),addSplit(3))
    })
    val df6=df5.toDF("Firstname","LastName","AddressLine","City","state","Zipcode")
    df6.printSchema()
    df6.show()
    df6.withColumn("zipcode",when(trim(col("zipcode"))===92537,999).otherwise(col(("zipcode")))).show()*/

    //====================================16/7/2020================================================================================================//
    logger.info("this fallowing code is practised on the date of 16/7/2020")
    import spark.implicits._

    val data1=spark.sparkContext.parallelize(List(("java",100000),("python",50000),("scala",800000)))
    val column=Seq("Language","user_count")
    column(0)
    column(1)
    val df7=spark.createDataFrame(data1).toDF(column:_*)
    df7.printSchema()
    df7.show()
    val dataa1=Seq(("java",100000),("python",500000000),("scala",800000000))

    val rdd3=spark.sparkContext.parallelize(dataa1)
    val df8=rdd3.toDF("language","userControl").show()
    val df9= dataa1.toDF("zzz","XXX").show()
    val df10=dataa1.toDF().show()

    val rowData=dataa1.map(x=>Row(x._1,x._2))
    println(rowData)
//    val schema_jul=StructType(column.map(k=>StructField(k,StringType,true)))
//    val df11=spark.createDataFrame(rowData,schema_jul)

    val csvDf=spark.read.csv("C:/Users/user/IdeaProjects/SparkSqlApis/sampleData.csv")
    csvDf.show()


















  }

}
