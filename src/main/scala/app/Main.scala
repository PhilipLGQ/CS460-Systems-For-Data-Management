package app

import app.analytics.SimpleAnalytics
import app.loaders.{MoviesLoader, RatingsLoader}
import app.recommender.LSH.{LSHIndex, NNLookup}
import app.recommender.baseline.BaselinePredictor
import app.recommender.collaborativeFiltering.CollaborativeFiltering
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK

object Main {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("app").setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)

    //your code goes here
    val loader_movies = new MoviesLoader(sc, path = "/movies_small.csv")
    val loader_ratings = new RatingsLoader(sc, path = "/ratings_small.csv")
    val rdd_movies = loader_movies.load()
    val rdd_ratings = loader_ratings.load()

    val processor = new SimpleAnalytics()
    processor.init(rdd_ratings, rdd_movies)

    //    val predictor = new BaselinePredictor()
    //    predictor.init(loader_ratings.load())
    //    println(predictor.predict(1, 260))

    // 4.5
//    val lsh = new LSHIndex(loader_movies.load(), IndexedSeq(5, 16))
//    val uid: Int = 16
//    val genre: List[String] = List("Action", "Adventure", "Sci-Fi")
//    val K: Int = 3
//
//    val nn_lookup = new NNLookup(lsh)
//    val baselinePredictor = new BaselinePredictor()
//    baselinePredictor.init(rdd_ratings)
//
//    val genre_lookup = sc.parallelize(Seq(genre))
//    genre_lookup.foreach(println)
//
//    val similarMovies = nn_lookup.lookup(genre_lookup)
//    similarMovies.foreach(println)
//
//    val predictions = similarMovies.flatMap {
//      case (_, movies) =>
//        movies.filter {
//          case (mid, _, keywords) =>
//            !baselinePredictor.getUserRating.contains(uid, mid) && keywords.nonEmpty
//        }
//    }.map {
//      case (movie_id, _, _) => (movie_id, baselinePredictor.predict(uid, movie_id))
//    }
//    predictions.foreach(println)
//
//    val result = predictions.top(K)(Ordering.by(_._2))
//    result.foreach(println)
  }
}
