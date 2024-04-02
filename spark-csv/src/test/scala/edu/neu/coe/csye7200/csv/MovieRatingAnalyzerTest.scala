import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class MovieRatingAnalysisSpec extends AnyFlatSpec with Matchers {

  // Initialize SparkSession for testing
  val spark: SparkSession = SparkSession.builder()
    .appName("MovieRatingAnalysisTest")
    .master("local[*]")
    .config("spark.driver.host", "localhost")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  // Sample test DataFrame
  val testDF: DataFrame = spark.createDataFrame(Seq(
    ("Movie1", 8.0),
    ("Movie1", 7.5),
    ("Movie2", 6.5),
    ("Movie3", 9.0),
    ("Movie3", 8.5)
  )).toDF("movie_title", "imdb_score")

  "processMovieRatings method" should "return tuple with expected values" in {
    val (meanRating, stdDevRating) = MovieRatingAnalysis.processMovieRatings(testDF)


    meanRating shouldEqual 7.9
    stdDevRating shouldEqual 0.96176 +- 0.1
  }

}
