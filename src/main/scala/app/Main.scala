package app

import app.analytics.SimpleAnalytics
import app.loaders.{MoviesLoader, RatingsLoader}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK

import java.text.SimpleDateFormat
import java.util.Date

object Main {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("app").setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)

    //your code goes here
    val loader_movies = new MoviesLoader(sc, path="/movies_small.csv")
    val loader_ratings = new RatingsLoader(sc, path="/ratings_small.csv")
    val rdd_movies = loader_movies.load()
    val rdd_ratings = loader_ratings.load()

    val processor = new SimpleAnalytics()
    processor.init(rdd_ratings, rdd_movies)
  }
}
