import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object MovieRatingAnalysis {

  def main(args: Array[String]): Unit = {
    // Create SparkSession
    val spark = SparkSession.builder()
      .appName("MovieRatingAnalysis")
      .master("local[*]")
      .config("spark.driver.host", "localhost")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val filePath = "C:\\CSYE7200\\Assignments\\CSYE7200\\spark-csv\\src\\main\\resources\\movie_metadata.csv" // Update with your file path
    val df = spark.read
      .option("header", "true")
      .csv(filePath)

    //println("Total count = " + df.count()) //Debugging code
    val resultDF = processMovieRatings(df)

    println(resultDF.count())

    resultDF.show(resultDF.count.toInt, false)


    spark.stop()
  }

  def processMovieRatings(df: DataFrame): DataFrame = {

    val meanDF = df.groupBy("movie_title")
      .agg(mean("imdb_score").alias("mean_rating"))


    val stdDevDF = df.groupBy("movie_title")
      .agg(stddev_samp("imdb_score").alias("std_dev_rating"))



//    Debugging code
//    val countDF = df.groupBy("movie_title")
//      .agg(count("movie_title").alias("count"))
//
//    val zeroCountMovies = countDF.filter(col("count") > 1)
//
//
//    if (zeroCountMovies.count() > 0) {
//      println("Movies with one count:" + zeroCountMovies.count())
//      zeroCountMovies.show()
//    } else {
//      println("No movies with zero count.")
//    }


    val resultDF = meanDF.join(stdDevDF, "movie_title")
    //val resultWithStdDevNotNull = resultDF.filter(col("std_dev_rating").isNotNull)  //debugging code
    resultDF
  }

}
