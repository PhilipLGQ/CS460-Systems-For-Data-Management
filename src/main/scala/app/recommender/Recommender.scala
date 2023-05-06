package app.recommender

import app.recommender.LSH.{LSHIndex, NNLookup}
import app.recommender.baseline.BaselinePredictor
import app.recommender.collaborativeFiltering.CollaborativeFiltering
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Class for performing recommendations
 */
class Recommender(sc: SparkContext,
                  index: LSHIndex,
                  ratings: RDD[(Int, Int, Option[Double], Double, Int)]) extends Serializable {

  sc.setCheckpointDir("target")
  private val nn_lookup = new NNLookup(index)
  private val collaborativePredictor = new CollaborativeFiltering(
    10, 0.1, 0, 4)
  collaborativePredictor.init(ratings)

  private val baselinePredictor = new BaselinePredictor()
  baselinePredictor.init(ratings)

  /**
   * Returns the top K recommendations for movies similar to the List of genres
   * for userID using the BaseLinePredictor
   */
  def recommendBaseline(userId: Int, genre: List[String], K: Int): List[(Int, Double)] = {
    val similarMovies = nn_lookup.lookup(sc.parallelize(Seq(genre)))
    val userRatings = baselinePredictor.getUserRating

    val poolMovies = similarMovies.flatMap {
      case (_, movies) =>
        movies.filter {
          case (movieId, _, keywords) => !userRatings.contains(userId, movieId) && keywords.nonEmpty }
    }.map {
      case (movieId, _, _) => movieId
    }.distinct.collect()

    val predictions = poolMovies
      .map {
        case movieId =>
          val prediction = baselinePredictor.predict(userId, movieId)
          (movieId, prediction)
      }

    predictions.sortBy(-_._2).take(K).toList
  }

  /**
   * The same as recommendBaseline, but using the CollaborativeFiltering predictor
   */
  def recommendCollaborative(userId: Int, genre: List[String], K: Int): List[(Int, Double)] = {
    val similarMovies = nn_lookup.lookup(sc.parallelize(Seq(genre)))
    val userRatings = collaborativePredictor.getUserRating

    val poolMovies = similarMovies.flatMap {
      case (_, movies) =>
        movies.filter {
          case (movieId, _, keywords) => !userRatings.contains(userId, movieId) && keywords.nonEmpty}
    }.map {
      case (movieId, _, _) => movieId
    }.distinct.collect()

    val predictions = poolMovies.map {
      case movieId =>
        val prediction = collaborativePredictor.predict(userId, movieId)
        (movieId, prediction)
    }

    predictions.sortBy(-_._2).take(K).toList
  }
}
