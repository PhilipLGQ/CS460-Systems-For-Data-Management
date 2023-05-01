package app.aggregator

import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK

/**
 * Class for computing the aggregates
 *
 * @param sc The Spark context for the given application
 */
class Aggregator(sc: SparkContext) extends Serializable {

  private var state: RDD[(Int, (String, List[String], Double, Int))] = null
  private var partitioner: HashPartitioner = null

  /**
   * Use the initial ratings and titles to compute the average rating for each title.
   * The average rating for unrated titles is 0.0
   *
   * @param ratings The RDD of ratings in the file
   *        format: (user_id: Int, title_id: Int, old_rating: Option[Double], rating: Double, timestamp: Int)
   * @param title   The RDD of titles in the file
   *
   */
  def init(
            ratings: RDD[(Int, Int, Option[Double], Double, Int)],
            title: RDD[(Int, String, List[String])]
          ): Unit = {
    // compute total rating and count for each movie title
    val titleRatingCount = ratings.flatMap {
      case (_, movie_id, old_rating, rating, _) =>
        old_rating match {
          case Some(old) => Seq((movie_id, (rating - old, 0)))
          case None => Seq((movie_id, (rating, 1)))
        }
    }.reduceByKey {
      case ((rating1, count1), (rating2, count2)) => (rating1 + rating2, count1 + count2)
    }.mapValues {
      case (total, count) => (total.toDouble / count, count)
    }

    // get list of keywords associated with each movie title
    val titleRemap = title.map {
      case (movie_id, movie_title, keywords) => (movie_id, (movie_title, keywords))
    }

    // join titleRatingCount with titleKeyword
    state = titleRemap.leftOuterJoin(titleRatingCount).mapValues {
      case ((movie_title, keywords), Some((rating, count))) => (movie_title, keywords, rating, count)
      case ((movie_title, keywords), None) => (movie_title, keywords, 0.0, 0)
    }
    partitioner = new HashPartitioner(state.partitions.length)

    // persist the resulting RDD in-memory
    state.persist(MEMORY_AND_DISK)
  }

  /**
   * Return pre-computed title-rating pairs.
   *
   * @return The pairs of titles and ratings
   */
  def getResult(): RDD[(String, Double)] = {
    val titleRating = state.map {
      case (_, (movie_title, _, rating, _)) => (movie_title, rating)
    }
    titleRating
  }

  /**
   * Compute the average rating across all (rated titles) that contain the
   * given keywords.
   *
   * @param keywords A list of keywords. The aggregate is computed across
   *                 titles that contain all the given keywords
   * @return The average rating for the given keywords. Return 0.0 if no
   *         such titles are rated and -1.0 if no such titles exist.
   */
  def getKeywordQueryResult(keywords: List[String]): Double = {
    // filter out all movie titles that don't contain all the given keywords
    val filteredResult = state.filter {
      case (_, (_, titleKeywords, _, _)) => keywords.forall(titleKeywords.contains)
    }

    if (filteredResult.isEmpty()) {
      -1.0
    } else {
      val ratings = filteredResult.map {
        case (_, (_, _, rating, _)) => (1, rating)
      }.reduce {
        case ((count1, rating1), (count2, rating2)) => (count1 + count2, rating1 + rating2)
      }
      ratings._2 / ratings._1
    }
  }

  /**
   * Use the "delta"-ratings to incrementally maintain the aggregate ratings
   *
   *  @param delta Delta ratings that haven't been included previously in aggregates
   *        format: (user_id: Int, title_id: Int, old_rating: Option[Double], rating: Double, timestamp: Int)
   */
  def updateResult(delta_ : Array[(Int, Int, Option[Double], Double, Int)]): Unit =
  {
    // convert Array to RDD
    val delta_RDD = sc.parallelize(delta_, state.partitions.length)

    // compute new total rating and count for each movie title
    val delta_titleRatingCount = delta_RDD.flatMap {
      case (_, movie_id, old_rating, rating, _) =>
        old_rating match {
          case Some(old) => Seq((movie_id, (rating - old, 0)))
          case None => Seq((movie_id, (rating, 1)))
        }
    }.reduceByKey {
      case ((rating1, count1), (rating2, count2)) => (rating1 + rating2, count1 + count2)
    }

    // update state with new ratings
    state = state.leftOuterJoin(delta_titleRatingCount).mapValues {
      case ((movie_title, keywords, old_avg_rating, old_count), Some((delta_rating, delta_count))) =>
        val new_count = old_count + delta_count
        val new_rating = (old_avg_rating * old_count + delta_rating) / new_count
        (movie_title, keywords, new_rating, new_count)
      case ((movie_title, keywords, old_avg_rating, old_count), None) =>
        (movie_title, keywords, old_avg_rating, old_count)
    }

    state.unpersist()
    state.persist(MEMORY_AND_DISK)
  }
}
