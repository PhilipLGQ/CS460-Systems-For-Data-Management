package app.recommender.LSH

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag

/**
 * Class for indexing the data for LSH
 *
 * @param data The title data to index
 * @param seed The seed to use for hashing keyword lists
 */
class LSHIndex(data: RDD[(Int, String, List[String])], seed: IndexedSeq[Int]) extends Serializable {

  private val partitioner = new HashPartitioner(data.partitions.length)
  private val minhash = new MinHash(seed)
  private val buckets = hash(data.map(_._3)).zip(data).map {
    case ((sig, _), (id, name, keywords)) => (sig, (id, name, keywords))
  }.groupByKey().mapValues(_.toList.sortBy(_._1))
    .partitionBy(partitioner)
    .cache()

  /**
   * Hash function for an RDD of queries.
   *
   * @param input The RDD of keyword lists
   * @return The RDD of (signature, keyword list) pairs
   */
  def hash(input: RDD[List[String]]): RDD[(IndexedSeq[Int], List[String])] = {
    input.map(x => (minhash.hash(x), x))
  }

  /**
   * Return data structure of LSH index for testing
   *
   * @return Data structure of LSH index
   */
  def getBuckets(): RDD[(IndexedSeq[Int], List[(Int, String, List[String])])] = {
    buckets
  }

  /**
   * Lookup operation on the LSH index
   *
   * @param queries The RDD of queries. Each query contains the pre-computed signature
   *                and a payload
   * @return The RDD of (signature, payload, result) triplets after performing the lookup.
   *         If no match exists in the LSH index, return an empty result list.
   */
  def lookup[T: ClassTag](queries: RDD[(IndexedSeq[Int], T)])
  : RDD[(IndexedSeq[Int], T, List[(Int, String, List[String])])] =
  {
    val local_buckets = buckets.collectAsMap()

    queries.flatMap {
      case (sig, payload) =>
        val matches = local_buckets.getOrElse(sig, Seq.empty)
        if (matches.nonEmpty) {
          Some((sig, payload, matches.toList))
        }
        else {
          Some((sig, payload, List.empty[(Int, String, List[String])]))
        }
    }
  }
}
