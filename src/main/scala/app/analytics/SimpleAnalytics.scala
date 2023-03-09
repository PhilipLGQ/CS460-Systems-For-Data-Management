package app.analytics

import org.apache.spark.HashPartitioner
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK
import org.joda.time.DateTime


class SimpleAnalytics() extends Serializable {

  private var ratingsPartitioner: HashPartitioner = null
  private var moviesPartitioner: HashPartitioner = null
  var titlesGroupedById: RDD[(Int, List[(String, List[String])])] = null
  var ratingsGroupedByYearByTitle: RDD[((Int, Int), List[(Int, Double)])] = null

  def init(
            ratings: RDD[(Int, Int, Option[Double], Double, Int)],
            movie: RDD[(Int, String, List[String])]
          ): Unit = {

    def TimestampToYear(timestamp: Long): Int = {
      //val year_format = new SimpleDateFormat("yyyy")
      val year_timestamp = new DateTime(timestamp * 1000L).toString("yyyy")
      year_timestamp.toInt
    }

    ratingsPartitioner = new HashPartitioner(10)
    moviesPartitioner = new HashPartitioner(10)
    titlesGroupedById = movie.groupBy(_._1)
      .mapValues(x => x.map(x => (x._2, x._3)))
      .mapValues(_.toList)
      .partitionBy(ratingsPartitioner)
      .persist(MEMORY_AND_DISK)
    ratingsGroupedByYearByTitle = ratings
      .map(x => ((TimestampToYear(x._5), x._2), (x._1, x._4)))
      .groupByKey()
      .mapValues(_.toList)
      .partitionBy(moviesPartitioner)
      .persist(MEMORY_AND_DISK)
  }

  def getNumberOfMoviesRatedEachYear: RDD[(Int, Int)] = {
    val return_value = ratingsGroupedByYearByTitle
      .map { case ((year, movie_id), ratinglist) => (year, movie_id) }
      .groupByKey()
      .mapValues(_.toSet.size)
    return_value
  }

  /**
   * Currently a tied problem to be determined by TAs
   */
  def getMostRatedMovieEachYear: RDD[(Int, String)] = {
    val ratings_max_count = ratingsGroupedByYearByTitle
      .flatMap { case ((year, movie_id), ratinglist) =>
        ratinglist.map { case (_, _) => ((year, movie_id), 1) }
      }
      .reduceByKey(_ + _)
      .map { case ((year, movie_id), count) => (year, (movie_id, count)) }
      .groupByKey()
      .mapValues { x =>
        x.toList.sortWith { case ((id_1, count_1), (id_2, count_2)) =>
          if (count_1 == count_2) id_1 > id_2 else count_1 > count_2
        }.head._1
      }
      .map(x => (x._2, x._1))

    val title_remap = titlesGroupedById
      .mapValues { movie_info =>
        val (title, keywords) = movie_info.head
        (title, keywords)
      }
      .map { case (movie_id, (title, _)) => (movie_id, title) }

    val return_value = ratings_max_count.join(title_remap)
      .map { case (_, (year, movie_title)) => (year, movie_title) }

    return_value
  }

  def getMostRatedGenreEachYear: RDD[(Int, List[String])] = {
    val ratings_max_count = ratingsGroupedByYearByTitle
      .flatMap { case ((year, movie_id), ratinglist) =>
        ratinglist.map { case (_, _) => ((year, movie_id), 1) }
      }
      .reduceByKey(_ + _)
      .map { case ((year, movie_id), count) => (year, (movie_id, count)) }
      .groupByKey()
      .mapValues { x =>
        x.toList.sortWith { case ((id_1, count_1), (id_2, count_2)) =>
          if (count_1 == count_2) id_1 > id_2 else count_1 > count_2
        }
          .head._1
      }
      .map(x => (x._2, x._1))

    val title_remap = titlesGroupedById
      .mapValues { movie_info =>
        val (title, keywords) = movie_info.head
        (title, keywords)
      }
      .map { case (movie_id, (_, keywords)) => (movie_id, keywords) }

    val return_value = ratings_max_count.join(title_remap)
      .map { case (_, (year, keywords)) => (year, keywords) }

    return_value
  }

  // Note: if two genre has the same number of rating, return the first one based on lexicographical sorting on genre.
  def getMostAndLeastRatedGenreAllTime: ((String, Int), (String, Int)) = {
    val ratings_max_count = ratingsGroupedByYearByTitle
      .flatMap { case ((year, movie_id), ratinglist) =>
        ratinglist.map { case (_, _) => ((year, movie_id), 1) }
      }
      .reduceByKey(_ + _)
      .map { case ((year, movie_id), count) => (year, (movie_id, count)) }
      .groupByKey()
      .mapValues { x =>
        x.toList.sortWith { case ((id_1, count_1), (id_2, count_2)) =>
          if (count_1 == count_2) id_1 > id_2 else count_1 > count_2
        }
          .head._1
      }
      .map(x => (x._2, x._1))

    val title_remap = titlesGroupedById
      .mapValues { movie_info =>
        val (title, keywords) = movie_info.head
        (title, keywords)
      }
      .map { case (movie_id, (_, keywords)) => (movie_id, keywords) }

    val keywords_freq_count = ratings_max_count.join(title_remap)
      .map { case (_, (year, keywords)) => (year, keywords) }
      .flatMap { case (_, keywords) => keywords
        .map(genre => (genre, 1))
      }
      .reduceByKey(_ + _)

    val most_freq_genre = keywords_freq_count.sortBy { case (genre, count) => (-count, genre) }.first()
    val least_freq_genre = keywords_freq_count.sortBy { case (genre, count) => (count, genre) }.first()

    (least_freq_genre, most_freq_genre)
  }

  /**
   * Filter the movies RDD having the required genres
   *
   * @param movies         RDD of movies dataset
   * @param requiredGenres RDD of genres to filter movies
   * @return The RDD for the movies which are in the supplied genres
   */
  def getAllMoviesByGenre(movies: RDD[(Int, String, List[String])],
                          requiredGenres: RDD[String]): RDD[String] = {
    val genre_set = requiredGenres.collect().toSet
    val return_value = movies
      .filter { case (_, _, keywords) => keywords.exists(genre_set.contains) }
      .map(x => x._2)

    return_value
  }

  /**
   * Filter the movies RDD having the required genres
   * HINT: use the broadcast callback to broadcast requiresGenres to all Spark executors
   *
   * @param movies            RDD of movies dataset
   * @param requiredGenres    List of genres to filter movies
   * @param broadcastCallback Callback function to broadcast variables to all Spark executors
   *                          (https://spark.apache.org/docs/2.4.8/rdd-programming-guide.html#broadcast-variables)
   * @return The RDD for the movies which are in the supplied genres
   */
  def getAllMoviesByGenre_usingBroadcast(movies: RDD[(Int, String, List[String])],
                                         requiredGenres: List[String],
                                         broadcastCallback: List[String] => Broadcast[List[String]]): RDD[String] = {
    val genre_broadcast = broadcastCallback(requiredGenres)
    val return_value = movies
      .filter(x => x._3.intersect(genre_broadcast.value).nonEmpty)
      .map(x => x._2)

    return_value
  }
}
