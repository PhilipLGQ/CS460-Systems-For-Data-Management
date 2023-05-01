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

  private val nn_lookup = new NNLookup(index)
//  private val collaborativePredictor = new CollaborativeFiltering(10, 0.1, 0, 4)
//  collaborativePredictor.init(ratings)

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
          case (movieId, _, keywords) =>
            !userRatings.contains(userId, movieId) && keywords.nonEmpty }
    }

    val predictions = poolMovies
      .map {
        case (movie_id, _, _) => (movie_id, baselinePredictor.predict(userId, movie_id)) }

    val result = predictions.top(K)(Ordering.by(_._2)).toList
    result
  }

  /**
   * The same as recommendBaseline, but using the CollaborativeFiltering predictor
   */
  def recommendCollaborative(userId: Int, genre: List[String], K: Int): List[(Int, Double)] = ???
}
