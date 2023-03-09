package app.loaders

import app.Main.getClass
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Helper class for loading the input
 *
 * @param sc The Spark context for the given application
 * @param path The path for the input file
 */
class RatingsLoader(sc : SparkContext, path : String) extends Serializable {

  /**
   * Read the rating file in the given path and convert it into an RDD
   *
   * @return The RDD for the given ratings
   */
  def load(): RDD[(Int, Int, Option[Double], Double, Int)] = {
    val file = getClass.getResource(path).getPath
    val rdd_ratings = sc.textFile(file)

    // Convert each line into a key-value pair RDD of ((user_id, movie_id), (rating, timestamp))
    val rdd_ratings_kv = rdd_ratings.map(line => {
      val values = line.split('|')
      val user_id = values(0).toInt
      val movie_id = values(1).toInt
      val rating = values(2).toDouble
      val timestamp = values(3).toInt
      ((user_id, movie_id), (rating, timestamp))
    })

    // Use reduceByKey to find the max timestamp for each (user_id, movie_id) group
    val rdd_max_timestamp = rdd_ratings_kv.reduceByKey((a, b) => if (a._2 > b._2) a else b)

    // Join the original RDD with the max timestamp RDD to get the old rating
    val rdd_joined = rdd_ratings_kv.join(rdd_max_timestamp)

    // Map the resulting RDD to the desired format of tuple (user_id, movie_id, old_rating, new_rating, timestamp)
    val rdd_ratings_tuples = rdd_joined.map({ case ((user_id, movie_id), ((new_rating, timestamp), (old_rating, max_timestamp))) =>
      val old_rating_update = if (timestamp == max_timestamp) None else Some(old_rating)
      (user_id, movie_id, old_rating_update, new_rating, timestamp)
    })

    rdd_ratings_tuples
  }
}

