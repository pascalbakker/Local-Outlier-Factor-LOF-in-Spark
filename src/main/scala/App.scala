package scala
// import org.apache.spark._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.SaveMode
import java.io.BufferedWriter
import java.io.FileWriter
import java.io.File
import scala.util.Random
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Vectors, Vector => Vec}
import breeze.plot._

object Main extends App{
  /*
    FOR GENERATING AND READING DATA
  */
  def generateDataset(r: Random, numRows: Int, numCols: Int, maxValue: Int):
  Iterator[List[Int]] = Iterator.fill(numRows)( List.fill(numCols)(r.nextInt(maxValue)))

  // Write data to disk. Store as csv
  def writeToDisk(data: Iterator[List[Int]], file_path: String):
  Unit = {
    val writer = new BufferedWriter(new FileWriter(file_path))
    data.map({
      case l: List[_] => l.mkString(",")+"\n"
      case _ => ""
    }).foreach(writer.write(_))
    writer.close()
  }

  /* PLOTTING FUNCTIONS */
  def plotInitialData(data: List[(Int,Int)]) = {
      val fig = Figure()
      val plt = fig.subplot(0)
      plt += scatter(x=data.map(_._1),y=data.map(_._2), { _ => 0.1 } )
  }

  /*
    LOF FUNCTIONS
  */

 // Preprocess RDD (String) -> (dID, Vec)
  def preprocess(rdd: RDD[String]):
  RDD[(Long,Vec)] = rdd.map(s => Vectors.dense(s.split(',').map(_.toDouble))).zipWithIndex().map(_.swap)

  // euclidian distance
  def distance(v1: Vec, v2: Vec):
  Double = math.sqrt(Vectors.sqdist(v1,v2))

  // Map job for neighborhoods
  def mapNeighborhoods(data: Array[(Long,Vec)], target: (Long,Vec), k: Int):
  (Long,Array[(Long,Double)]) = {
    val distances = data.map{ case (neighborId: Long, vec: Vec) => (neighborId, distance(target._2,vec))}
    (target._1, distances)
  }

  //neighborhoodRDD (dID, vector) -> (dID, Array(neighborId, dist))
  def neighborhood(spark: SparkSession, data: RDD[(Long,Vec)], k: Int):
  RDD[(Long, Array[(Long, Double)])] = {
    // Broadcast each partition
    val kneighbors = Range(0,data.getNumPartitions).map{ partNum: Int =>
      val iteratorData = data.mapPartitionsWithIndex( (index, iterator) => {
          if(index==partNum) iterator
          else Iterator.empty
      }).collect()

      // MAP: compute all neighborhoods
      val broadcastedData = spark.sparkContext.broadcast(iteratorData)
      val neighborhoodsMapped = data.mapPartitions{ iterator => {
         val partitionData = iterator.toArray
         val neighborhoodList = new ArrayBuffer[(Long,Array[(Long,Double)])]()
         broadcastedData.value.foreach{ case (targetId: Long, targetVec: Vec) =>
              val neighborhood = mapNeighborhoods(partitionData,(targetId,targetVec),k)
              neighborhoodList.append(neighborhood)
         }
         neighborhoodList.iterator
        }
      }
      // COMBINE: all neighborhood
      def reduceNeighborhoods(neighborhood1: Array[(Long,Double)], neighborhood2: Array[(Long,Double)]):
      Array[(Long,Double)] = {
          val merged =neighborhood1.sortBy(_._2).slice(0,k) ++ neighborhood2.sortBy(_._2).slice(0,k)
          merged.sortBy(_._2).slice(0,k)
        }
      val combinedNeighborhoods = neighborhoodsMapped.reduceByKey(reduceNeighborhoods)
      combinedNeighborhoods
    }.reduce(_.union(_))
    kneighbors
  }

  // neighborhoodReverseRDD (dID, Array(neighborID, dist)) => (neighborID, Array(dId,distance))
  def neighborhoodReverse(data: RDD[(Long,Array[(Long, Double)])]):
  RDD[(Long,Iterable[(Long,Double)])] = {
    val reversedRDD = data.flatMap{
          case (dID: Long, neighborhood: Array[(Long,Double)]) => neighborhood.map{
            case (neighborID: Long, distance: Double) => (neighborID,(dID,distance))
          }
    }.groupByKey()
    reversedRDD
  }

  // kDistance (dID, Array(neighborID, dist)) => (dID, kDist)
  def kDistance(data: RDD[(Long,Array[(Long, Double)])], k: Int):
  RDD[(Long,Double)] =  {
    val kDistance = data.mapPartitions(iterable => {
        val result = iterable.map{
          case (dID: Long, neighborhood: Array[(Long,Double)]) => (dID, neighborhood(k-1)._2)
        }
        result
    })
    kDistance
  }

  // lrd (reversedRDD, kDistanceRDD) => (dID, Double)
  def lrd(kDistanceRDD: RDD[(Long,Double)], reverseRDD: RDD[(Long,Iterable[(Long,Double)])], k: Int):
  RDD[(Long,Double)] = {
    val joined = kDistanceRDD.join(reverseRDD)
    // (dID, (kdistance, Array[(neighborID, distance)]))
    val lrdRDD = joined.mapPartitions( iterator => {
      val result = iterator.map{
        case (k: Long, v: (Double, Iterable[(Long,Double)])) => {
          val sumOfDistances = v._2.foldLeft(0d){ (accum,b) =>
            val maxval = if(b._2 > v._1) b._2 else v._1
            accum + maxval
          }
          (k, 1/(sumOfDistances/k))
        }
      }
      result
    })
    lrdRDD
  }

  def lof(lrdRDD: RDD[(Long,Double)], reverseRDD: RDD[(Long,Iterable[(Long,Double)])]): RDD[(Long,Double)] = {
    val joined = reverseRDD.join(lrdRDD).mapValues{
      case (distances, lrd) => {
          val lrdList = distances.map{
            case (index, _) => (index, lrd)
          }.toArray
          lrdList.map(_._2).sum / lrdList.length
      }
    }
    joined
  }

  override def main(args: Array[String]) = {
    val k = 3
    val data_location = "data/data.csv"
    val data = generateDataset(new Random(1), 20, 2, 100) //Generate 100 rows of data of 2 columns
    //generateNewData(data_location)
    plotInitialData(data.toList.map{case List(a,b) => (a,b)})
    val spark: SparkSession = SparkSession.builder()
        .master("local")
        .appName("LOF")
        .getOrCreate()
    val rdd_string = spark.sparkContext.textFile(data_location)
    val rdd_vector = preprocess(rdd_string)
    val kneighbors = neighborhood(spark,  rdd_vector,k)
    // parallel
    val neighborhoodReverseRDD = neighborhoodReverse(kneighbors)
    val kdistanceRDD = kDistance(kneighbors,k)
    // Final
    val lrdRDD = lrd(kdistanceRDD, neighborhoodReverseRDD,k)
    val lofRDD = lof(lrdRDD, neighborhoodReverseRDD)
    val toStringRDD = lofRDD.map(_.toString())
    //lofRDD.map(_.toString())
    //kneighbors.map(_.toString()).saveAsTextFile("data/kneighbors.txt")
    toStringRDD.saveAsTextFile("data/lrd.txt")
    spark.sparkContext.stop()
  }
}
