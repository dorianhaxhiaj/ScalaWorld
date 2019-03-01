package org.abc.com

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}
import java.io.File


object HelloScala {

  def main(args: Array[String]) {


    val dirSrcPath = "src/main/resources/data-spark"
    val dirDestPath = "/tmp/data-spark"

    val sparkSession = SparkSession.builder()
      .appName("example-spark-scala-read-and-write-from-hdfs")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/tmp")
      .config("hive.metastore.warehouse.dir", "file:///C:/tmp/hive")
      .enableHiveSupport()
      .getOrCreate()

    val dirOne = new File("src/main/resources/data-spark")
    val listOfFiles = dirOne.listFiles
      .filter(_.isFile)
      .toList

    import sparkSession.sql


    for (file <- listOfFiles) {
      val tableName: String = file.getName.substring(0, file.getName.indexOf("."))
      val df_csv = sparkSession.read.option("inferSchema", "true").csv(dirSrcPath + "/" + file.getName)
      df_csv.write.mode(SaveMode.Overwrite).csv(dirDestPath + "/" + tableName)
      sql("DROP TABLE IF EXISTS " + tableName)
      df_csv.write.mode(SaveMode.Overwrite) saveAsTable (tableName)
    }


    val dataFrameAgregate = sql("SELECT d._c0 as DriverId, d._c1 as Name, t.Hours_Logged, t.Miles_Logged from drivers d " +
      "INNER JOIN (SELECT _c0, sum(`_c2`) as Hours_Logged , sum(`_c3`) as Miles_Logged FROM timesheet GROUP BY _c0 ) t ON (d._c0 = t._c0)")


    dataFrameAgregate.show()
    sparkSession.close()

  }


}
