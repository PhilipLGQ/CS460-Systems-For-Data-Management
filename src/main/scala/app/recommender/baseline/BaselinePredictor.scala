package app.recommender.baseline

import org.apache.spark.rdd.RDD

class BaselinePredictor() extends Serializable {

  private var state: Map[Int, Double] = Map()
  private var userRatings: Map[(Int, Int), Double] = Map()
  private var globalAverageDeviation: Double = 0.0
  private var globalAverageDeviationsByMovie: Map[Int, Double] = Map()

  def init(ratingsRDD: RDD[(Int, Int, Option[Double], Double, Int)]): Unit = {
    val titleRatingCountUser = ratingsRDD.flatMap {
      case (user_id, movie_id, old_rating, rating, _) =>
        old_rating match {
          case Some(old) => Seq(((user_id, movie_id), (rating - old, 0)))
          case None => Seq(((user_id, movie_id), (rating, 1)))
        }
    }.reduceByKey {
      case ((rating1, count1), (rating2, count2)) => (rating1 + rating2, count1 + count2)
    }

    // Compute the average rating by user
    val averageRatingByUser = titleRatingCountUser.map {
      case ((user_id, _), (rating, count)) => (user_id, (rating, count))
    }.reduceByKey {
      case ((rating1, count1), (rating2, count2)) => (rating1 + rating2, count1 + count2)
    }.mapValues {
      case (total, count) => total / count
    }.collectAsMap().toMap

    // normalized deviation for each rating
    val deviations = titleRatingCountUser.mapValues {
      case (rating, _) => rating
    }.map {
      case ((user_id, movie_id), rating) =>
        ((user_id, movie_id), (rating - averageRatingByUser(user_id)) / scale(rating, averageRatingByUser(user_id)))
    }

    // global average deviation for each movie
    globalAverageDeviationsByMovie = deviations
      .map { case ((_, movie_id), deviation) => (movie_id, deviation) }
      .groupByKey()
      .mapValues(deviations => deviations.sum / deviations.size)
      .collectAsMap().toMap

    // global average deviation for all movies
    globalAverageDeviation = deviations
      .map { case ((_, _), deviation) => deviation }
      .sum() / deviations.count()

    // fast access to user average ratings and user rating entries
    state = averageRatingByUser
    userRatings = titleRatingCountUser.mapValues {
      case (rating, _) => rating
    }.collectAsMap().toMap
  }

  def getUserRating: Map[(Int, Int), Double] = {
    userRatings
  }

  def scale(x: Double, userAvgRating: Double): Double = {
    if (x > userAvgRating) {
      5.0 - userAvgRating
    } else if (x < userAvgRating) {
      userAvgRating - 1.0
    } else {
      1.0
    }
  }

  def predict(userId: Int, movieId: Int): Double = {
    if (!state.contains(userId)) {
      return globalAverageDeviation
    }
    val userAvgRating = state(userId)
    if (!globalAverageDeviationsByMovie.contains(movieId)) {
      return userAvgRating
    }

    val globalAvgDeviationForMovie = globalAverageDeviationsByMovie(movieId)
    val prediction = userAvgRating + globalAvgDeviationForMovie * scale(
      userAvgRating + globalAvgDeviationForMovie, userAvgRating)

    prediction
  }
}
