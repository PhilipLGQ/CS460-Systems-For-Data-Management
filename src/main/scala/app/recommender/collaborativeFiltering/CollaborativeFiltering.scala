package app.recommender.collaborativeFiltering

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}


class CollaborativeFiltering(rank: Int,
                             regularizationParameter: Double,
                             seed: Long,
                             n_parallel: Int) extends Serializable {

  // NOTE: set the parameters according to the project description to get reproducible (deterministic) results.
  private val maxIterations = 20
  private var model: MatrixFactorizationModel = null

  def init(ratingsRDD: RDD[(Int, Int, Option[Double], Double, Int)]): Unit = {
    val titleUserMovieLatestRating = ratingsRDD.flatMap {
      case (user_id, movie_id, old_rating, rating, _) =>
        old_rating match {
          case Some(old) => Seq(((user_id, movie_id), (rating - old, 0)))
          case None => Seq(((user_id, movie_id), (rating, 1)))
        }
    }.reduceByKey {
      case ((rating1, count1), (rating2, count2)) => (rating1 + rating2, count1 + count2)
    }.map {
      case ((user_id, movie_id), (rating, _)) => (user_id, movie_id, rating)
    }

    val ratings = titleUserMovieLatestRating.map {
      case (user_id, movie_id, rating) => Rating(user_id, movie_id, rating)
    }

    model = ALS.train(
      ratings = ratings,
      rank = rank,
      iterations = maxIterations,
      lambda = regularizationParameter,
      blocks = n_parallel,
      seed = seed
    )
  }

  def predict(userId: Int, movieId: Int): Double = {
    model.predict(userId, movieId)
  }
}
