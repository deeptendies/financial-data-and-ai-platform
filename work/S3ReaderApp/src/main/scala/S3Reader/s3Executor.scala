package S3Reader

import org.apache.spark.rdd.RDD
//  ref https://sparkbyexamples.com/spark/spark-read-text-file-from-s3/
object s3Executor {
  def execute(args: Array[String]): RDD[String] = {
    import org.apache.spark.sql.SparkSession
    val accessKeyID = args(0) //  accessKeyID
    val secretAccessKey = args(1) //  secretAccessKey
    val s3Path = args(2) // s3Path
    val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName("SparkByExamples.com")
      .getOrCreate()
    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.access.key", accessKeyID)
    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.secret.key", secretAccessKey)
    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")
    val s3RDD = spark.sparkContext.textFile(s3Path)
    s3RDD
  }


  def main(args: Array[String]): Unit = {
    // define commons, best to get credentials from environment variables
    val accessKeyID = System.getenv("ACCESS_KEY_ID")
    val secretAccessKey = System.getenv("SECRET_ACCESS_KEY")
    val s3Path = "s3a://skip-capstone-2022/grocery_order_transaction_data/*.json"
    val s3ResultRDD = execute(Array(accessKeyID,
      secretAccessKey,
      s3Path
    ))
    println("# print rdd class")
    println(s3ResultRDD.getClass)
    println("# print collect forech f")
    s3ResultRDD.collect().foreach(println)
    s3ResultRDD.collect().foreach(f => {
      println(f)
    })
  }
}
