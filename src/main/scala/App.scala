package scala
// import org.apache.spark._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import scala.reflect.io.Directory
import java.io.File
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
import org.apache.spark.SparkContext._

object Main extends App{
  /*
    FOR GENERATING AND READING DATA
  */
  def generateInCircle(rand: Random, radius: Double, centerX: Double, centerY: Double, numPoints: Int):
  List[(Double,Double)] = {
    List.fill(numPoints){
        val a = rand.nextDouble() * 2 * Math.PI
        val r = radius * Math.sqrt(rand.nextDouble())
        val x = r * Math.cos(a) + centerX
        val y = r * Math.sin(a) + centerY
        (x,y)
    }
  }

  // Write data to disk. Store as csv
  def writeToDisk(data: List[(Double,Double)], file_path: String):
  Unit = {
    val writer = new BufferedWriter(new FileWriter(file_path))
      data.map {
        case (x,y) => x.toString + "," + y.toString +"\n"
        case _ => ""
      }.foreach(writer.write(_))
    writer.close()
  }

  // Use this to generate differnt datasets
  def generateDataForTesting(): List[(Double,Double)]= {
    val filepath = "data/data.csv"
    val rand = new Random(1) // seed 1
    val circle1 = generateInCircle(rand, 5d, 10d, 10d, 1000)
    val circle2 = generateInCircle(rand, 20d, 100d, 100d, 1000)
    val circle3 = generateInCircle(rand, 20d, 50d, 50d, 10)
    val data = circle1 ::: circle2 ::: circle3
    writeToDisk(data, filepath)
    data
  }

  /* PLOTTING FUNCTIONS */
  def plotInitialData(data: List[(Double,Double)]) = {
      // plot original scatter plot of data
      val fig = Figure()
      val plt = fig.subplot(0)
      plt += scatter(x=data.map(_._1),y=data.map(_._2), { _ => 0.1 } )
      // Plot results with color

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


def combineNeighborhood(
      first: Array[(Long, Double)],
      second: Array[(Long, Double)]): Array[(Long, Double)] = {

    val minPts = 3
    var pos1 = 0
    var pos2 = 0
    var count = 0 // the size of distinct instances
    val combined = new ArrayBuffer[(Long, Double)]()

    while (pos1 < first.length && pos2 < second.length && count < minPts) {
        if (first(pos1)._2 == second(pos2)._2) {
          combined.append(first(pos1))
          pos1 += 1
          if (combined.length == 1) {
            count += 1
          } else {
            if (combined(combined.length - 1) != combined(combined.length - 2)) {
              count += 1
            }
          }
          combined.append(second(pos2))
          pos2 += 1
        } else {
          if (first(pos1)._2 < second(pos2)._2) {
            combined.append(first(pos1))
            pos1 += 1
          } else {
            combined.append(second(pos2))
            pos2 += 1
          }
          if (combined.length == 1) {
            count += 1
          } else {
            if (combined(combined.length - 1) != combined(combined.length - 2)) {
              count += 1
            }
          }
        }
    }

    while (pos1 < first.length && count < minPts) {
      combined.append(first(pos1))
      pos1 += 1
      if (combined.length == 1) {
        count += 1
      } else {
        if (combined(combined.length - 1) != combined(combined.length - 2)) {
          count += 1
        }
      }
    }

    while (pos2 < second.length && count < minPts) {
      combined.append(second(pos2))
      pos2 += 1
      if (combined.length == 1) {
        count += 1
      } else {
        if (combined(combined.length - 1) != combined(combined.length - 2)) {
          count += 1
        }
      }
    }
    combined.toArray
  }

  // Map job for neighborhoods
  def mapNeighborhoods(data: Array[(Long,Vec)], target: (Long,Vec), k: Int):
  (Long,Array[(Long,Double)]) = {
    val distances = data.map{
      case (neighborId: Long, vec: Vec) => (neighborId, distance(target._2,vec))
    }
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
         broadcastedData.value.foreach{ case (targetId: Long, targetVec: Vec) => {
              val neighborhood = mapNeighborhoods(partitionData,(targetId,targetVec),k)
              neighborhoodList.append(neighborhood)
         }}
         neighborhoodList.iterator
        }
      }
      // COMBINE: all neighborhood
      def reduceNeighborhoods(neighborhood1: Array[(Long,Double)], neighborhood2: Array[(Long,Double)]):
      Array[(Long,Double)] = {
          val merged =neighborhood1.sortBy(- _._2).slice(0,k-1) ++ neighborhood2.sortBy(- _._2).slice(0,k-1)
          merged.sortBy(- _._2).slice(0,k-1)
        }
      val combinedNeighborhoods = neighborhoodsMapped.reduceByKey(combineNeighborhood)
      combinedNeighborhoods
    }.reduce(_.union(_))
    kneighbors
  }

  // neighborhoodReverseRDD (dID, Array(neighborID, dist)) => (neighborID, Array(dId,distance))
  def neighborhoodReverse(data: RDD[(Long,Array[(Long, Double)])]):
  RDD[(Long,Array[(Long,Double)])] = {
    val reversedRDD = data.flatMap{
          case (dID: Long, neighborhood: Array[(Long,Double)]) => neighborhood.map{
            case (neighborID: Long, distance: Double) => (neighborID,(dID,distance))
          } // :+ (dID, (dID,0d))
    }.groupByKey()
    reversedRDD.map{
      case(a,b)=>(a,b.toArray)
    }
  }

  def lrdReduceByKey(p1: (Double,Int), p2: (Double, Int)): (Double, Int) = {
      (p1._1 + p2._1, p1._2 + p2._2)
  }

  def lrd(neighborhoodReverseRDD: RDD[(Long,Array[(Long,Double)])], neighborhoodRDD: RDD[(Long,Array[(Long,Double)])]):
  RDD[(Long,Double)] = {
    val joined = neighborhoodReverseRDD.cogroup(neighborhoodRDD)
    joined.flatMap{
      case (outIdx: Long, (k:Iterable[Array[(Long,Double)]], v: Iterable[Array[(Long,Double)]])) => {
          require(k.size == 1 && v.size == 1)
          val kDistance = v.head.last._2
          k.head.filter(_._1 != outIdx).map{
            case(inIdx: Long, dist:Double) =>{
                (inIdx, (outIdx,Math.max(dist,kDistance)))
            }
          }
      }
    }.groupByKey().map{
      case(idx:Long, iter: Iterable[(Long,Double)]) =>{
          val num = iter.size
          val sum = iter.map(_._2).sum
          (idx,num/sum)
      }
    }
  }

  def neighborAverage(reverseRDD: RDD[(Long,Array[(Long,Double)])], lrdRDD: RDD[(Long,Double)]):
  RDD[(Long,Double)] = {
    val joined = reverseRDD.cogroup(lrdRDD)
    joined.flatMap {
      case (outIdx: Long, (v: Iterable[Array[(Long, Double)]], k: Iterable[Double])) =>
        require(k.size == 1 && v.size == 1)
        val lrd = k.head
        v.head.map { case (inIdx: Long, dist: Double) =>
          (inIdx, (outIdx, lrd))
        }
    }.groupByKey().map { case (idx: Long, iter: Iterable[(Long, Double)]) =>
      val lrd = iter.find(_._1 == idx).get._2
      val sum = iter.filter(_._1 != idx).map(_._2).sum
      (idx, sum / lrd / (iter.size - 1))
    }
  }

  def lof(averageLRDNeighborhoodRDD: RDD[(Long,Double)], lrdRDD: RDD[(Long,Double)]):
  RDD[(Long,Double)] = {
    val joinedRDD = averageLRDNeighborhoodRDD.join(lrdRDD)
    joinedRDD.map{
      case (dID: Long, (average: Double,lrd: Double)) =>{
        (dID,average/lrd)
      }
    }
  }

  def apply(spark:SparkSession, rdd_vector: RDD[(Long,Vec)], k:Int=3): RDD[(Long,Double)] = {
    val kneighbors = neighborhood(spark,  rdd_vector,k)
    val neighborhoodReverseRDD = neighborhoodReverse(kneighbors).persist()
    /*
    val reverse = neighborhoodReverseRDD.map{
      case(a,b) => (a,b.toArray)
    }
    */
    val lrdRDD = lrd(neighborhoodReverseRDD, kneighbors).persist()
    val averageLRDNeighborhoodRDD = neighborAverage(neighborhoodReverseRDD, lrdRDD)
    val lofRDD = lof(averageLRDNeighborhoodRDD,lrdRDD)
    // Write results
    // Convert to string
    val toStringlofRDD = lofRDD.map{
      case(a,b) => a.toString() + "," + b.toString()
    }
    val toStringaverageRDD = averageLRDNeighborhoodRDD.map{
      case(a,b) => a.toString +","+b.toString
    }
    val toStringLRDRDD = lrdRDD.map{
      case (neighborID: Long, lrd: Double) => neighborID.toString + "," + lrd.toString
    }
    toStringaverageRDD.saveAsTextFile("data/results/averageLRD.txt")
    toStringLRDRDD.saveAsTextFile("data/results/lrd.txt")
    toStringlofRDD.saveAsTextFile("data/results/lof.txt")
    averageLRDNeighborhoodRDD
  }

  def removeResults(): Unit = {
    val directory = new Directory(new File("data/results/"))
    directory.deleteRecursively()
  }


  override def main(args: Array[String]) = {
    val k = 100
    val data_location = "data/data.csv"
    removeResults()
    //val data = generateDataset(new Random(1), 20, 2, 100) //Generate 100 rows of data of 2 columns
    //writeToDisk(data,data_location)
    if(true){
      val data = generateDataForTesting()
    }
    //plotInitialData(data)
    val spark: SparkSession = SparkSession.builder()
        .master("local")
        .appName("LOF")
        .config("eventLog",true)
        .getOrCreate()

    val rdd_string = spark.sparkContext.textFile(data_location)
    val rdd_vector = preprocess(rdd_string)
    // Prepares the RDD for LOF
    val lofRDD = apply(spark, rdd_vector, k)
  }
}
