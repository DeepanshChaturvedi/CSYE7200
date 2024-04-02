import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}

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
    val (meanRating, stdDevRating) = processMovieRatings(df)
    println("Mean Rating for all movies: " + meanRating)
    println("Standard Deviation Rating of all movies: " + stdDevRating)

    spark.stop()
  }

  def processMovieRatings(df: DataFrame): (Double, Double) = {
    val meanDF = df.agg(mean("imdb_score").alias("mean_rating"))
    val stdDevDF = df.agg(stddev_samp("imdb_score").alias("std_dev_rating"))

    val meanRating = meanDF.select("mean_rating").as[Double](Encoders.scalaDouble).first()
    val stdDevRating = stdDevDF.select("std_dev_rating").as[Double](Encoders.scalaDouble).first()

    (meanRating, stdDevRating)
  }
}
