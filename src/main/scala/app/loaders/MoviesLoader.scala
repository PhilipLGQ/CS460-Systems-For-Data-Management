package app.loaders

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Helper class for loading the input
 *
 * @param sc   The Spark context for the given application
 * @param path The path for the input file
 */
class MoviesLoader(sc: SparkContext, path: String) extends Serializable {

  /**
   * Read the title file in the given path and convert it into an RDD
   *
   * @return The RDD for the given titles
   */
  def load(): RDD[(Int, String, List[String])] = {
    val file = getClass.getResource(path).getPath
    val rdd_ratings = sc.textFile(file)
    val rdd_movie = rdd_ratings.map(line => {
      val value = line.split("\\|")
      val movie_id = value(0).toInt
      val movie_title = value(1).replaceAll("\"", "").toString
      val movie_keyword = value.drop(2).map(_.replaceAll("\"", "")).toList
      (movie_id, movie_title, movie_keyword)
    })
    rdd_movie
  }
}

