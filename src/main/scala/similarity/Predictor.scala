package similarity
import toPair._
import averaging._
import collectionMap._
import time._

import org.rogach.scallop._
import org.json4s.jackson.Serialization
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val train = opt[String](required = true)
  val test = opt[String](required = true)
  val json = opt[String]()
  verify()
}

case class Rating(user: Int, item: Int, rating: Double)
/*
Time object  :

@param : List of time measurements : List[Int]
return : sugar functions of average and standard deviation computation
on time measurements
 */

object time {

  implicit class time_stats (times : List[Double]) {
    // return average on list of time measurements
    def avg  :Double  = times.sum/times.length

    // return standard deviation  on list of time measurements
  def stdev: Double  = {
    val avgTime = times.avg
    scala.math.sqrt(times.map(r=> scala.math.pow(r-avgTime,2)).sum/ times.length)
  }
  }
}

/*
to Pair object  :

@param : rdd of rating
return : sugar functions to perform mapping on rdd for later use
 */

object toPair {

  implicit class paired_functions (data : RDD[Rating]) {
    def userRating : RDD[(Int,Double)] = data.map(r=>(r.user,r.rating))
    def itemRating : RDD[(Int,Double)]  = data.map(r=>(r.item,r.rating))
    def mapping : RDD[(Int,(Int,Double))] = data.map(r => (r.user, (r.item, r.rating)))
    def mappingItem : RDD[(Int,(Int,Double))] = data.map(r => (r.item, (r.user, r.rating)))

    def ratingAvg : Double  = data.map(_.rating).mean
  }
}

/*
Averaging object  :

@param : RDD[(Int, Double)]
return : sugar functions to perform average rating per user or per item
 */
object averaging {

  implicit class averageFunctions(data: RDD[(Int, Double)]) {
    /* Compute average per key of rdd
     */
    def featureAverage: RDD[(Int, Double)] = {
      data.aggregateByKey((0.0, 0))(
        (k, v) => (k._1 + v, k._2 + 1),
        (v1, v2) => (v1._1 + v2._1, v1._2 + v2._2))
        .mapValues(sum => 1.0 * sum._1 / sum._2.toDouble)
    }
  }
  }

/*
Timer object  :
return : sugar functions for timing operations
 */

object Timer {


  // return time measurements for prediction and similarity
  def timer(t: => Any): (List[Double],List[Double])= {
    val times = (1 to 5).map {r =>
      val start = System.nanoTime()
      val _ = t
      val end = System.nanoTime()
      ((end - start) / 1e3, simiTime)
    }.toList
    (times.map(_._1),times.map(_._2))
  }

  // update similarity time measurement when called
  def updateSimiTime (value : Double) :Unit = {
    simiTime = simiTime  + (value/ 1e3)
  }



/*
 variable to update similarity time measurement
 at each iteration
 */
  var simiTime = 0.0
}

/*
Similarity object

return: the item intersection for each similarity
computation

 */
object Similarities {


  // sum the size intersection for all similarities for user u
  def multiplicationsTotal  : Double ={
    similarityItems.filter(_ !=0).sum
  }
  // update the similarity list with  item intersection with user v
  def updateItems (item : Double): Unit ={
    similarityItems =  item ::  similarityItems
  }
  // initialize item intersection list
  var similarityItems = List.empty[Double]

  var nonZeroSimi = 0

}
/* collection Map object

@param : map of similarities / normalized ratings : Map[(Int,Int), Double]
return sugar functions to compute max, min average
and standard deviation
 */
object collectionMap {

  implicit class collectionFunctions (data : Map[(Int,Int), Double]){
    // retrieve items if normalized ratings passed
    def getItems: Set[Int]  = {
      data.keySet.map(_._2)
    }
    // compute average
    def average : Double = data.values.sum / data.size

    // compute min
    def minValue : Double = {
      data.values.min
    }
    // compute max
    def maxValue : Double = {
      data.values.max
    }
    // compute standard deviation
    def stdDev : Double = {
      scala.math.sqrt( data.values.map(r=> scala.math.pow(r-data.average,2)).sum /data.size)
    }
  }
}

  object Predictor extends App {
    // Remove these lines if encountering/debugging Spark
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark = SparkSession.builder()
      .master("local[1]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    println("")
    println("******************************************************")

    var conf = new Conf(args)
    println("Loading training data from: " + conf.train())
    val trainFile = spark.sparkContext.textFile(conf.train())
    val train = trainFile.map(l => {
      val cols = l.split("\t").map(_.trim)
      Rating(cols(0).toInt, cols(1).toInt, cols(2).toDouble)
    })
    assert(train.count == 80000, "Invalid training data")

    println("Loading test data from: " + conf.test())
    val testFile = spark.sparkContext.textFile(conf.test())
    val test = testFile.map(l => {
      val cols = l.split("\t").map(_.trim)
      Rating(cols(0).toInt, cols(1).toInt, cols(2).toDouble)
    })
    assert(test.count == 20000, "Invalid test data")

    // Save answers as JSON
    def printToFile(content: String,
                    location: String = "./answers.json") =
      Some(new java.io.PrintWriter(location)).foreach {
        f =>
          try {
            f.write(content)
          } finally {
            f.close
          }
      }
/*
 scale function
 @param : user specific weighted sum deviation : Double
  @param : user average : Double
 @ return : rescaled rating
 */
    def scale(x: Double, user_avg: Double): Double = {
      x match {
        case _ if x > user_avg => 5.0 - user_avg
        case _ if x < user_avg => user_avg - 1.0
        case _ if x == user_avg => 1.0
      }
    }

    /*
    Computes the item intersection between user u and v
    @param : item set for user u and b
    return list of item present in their intersection
     */
    def itemIntersection(itemU: Set[Int], itemV: Set[Int]) = {
      itemU.intersect(itemV).toList
    }
    // Initialize empty rdd for adjusted cosine prediction
    var predictionAdjustedCosine = spark.sparkContext.emptyRDD[(Int, Int, Double, Double)]
    // Initialize empty rdd for jaccard prediction
    var predictionJacquard = spark.sparkContext.emptyRDD[(Int, Int, Double, Double)]

    /* Normalized deviation
  *
  *Compute the normalized deviation for each rating
  *@param RDD[Rating]
  * @param average user rating : RDD[(Int, Double)]
  *return rdd of normalized ratings RDD[Rating]
   */

    def normalizedDeviation(data: RDD[Rating], averageUserRating: RDD[(Int, Double)]) = {
      // map rdd to user and ratings pairs
      data.mapping
        // join with average users ratings
        .join(averageUserRating)
        // compute the normalized deviation between rating and average user
        .map { case (user, ((item, rating), avgUser)) => Rating(user, item, 1.0 * (rating - avgUser) / scale(rating, avgUser)) }
        // for later use
        .persist()
    }

    /* Preprocess ratings
  *
  * Compute preprocessed ratings
  *@param rdd of ratings
  *return preprocessed ratings
   */
    def preprocessRating(data: RDD[Rating]) = {

      // compute user average ratings
      val averageUserRatings = data.userRating.featureAverage
      // compute normalized ratings
      val normalizedRatings = normalizedDeviation(data, averageUserRatings)
      // map rdd to user  and rating pair
      val normalizer = normalizedRatings.userRating
        // compute square of ratings
        .mapValues(r => scala.math.pow(r, 2))
        // sum squared ratings per user
        .reduceByKey(_ + _)
        // square root of the sum of squared ratings per user
        .mapValues(r => scala.math.sqrt(r))

              // map to user as key for rdd join
      val x = normalizedRatings.mapping
        // join normalizer (denominator in normalized rating equation)
        .join(normalizer)
        // compute normalized ratings
        .map { case (user, ((item, normDeviation), normalizer)) => ((user, item), (normDeviation, 1.0 * normDeviation / normalizer)) }
      // put to map for similarity computation
      x.collect().toMap
    }

    /* Compute minimum number of s_u,v_S
    @param total number of users
    return minimum of similarity computations
     */
    def computationsUV (U : Int ) ={
      // for every user
      (1 to U).map( i =>
        // all other users inferior to him
        (1 to i-1).map(_=>1)).reduce(_ ++ _).size
    }

    /* Compute similarities

    @param: preprocessed ratings Map[Int,Map[(Int, Int), Double]],
    @user of interest Int
     metric choice, neighbors k
     @ return  if k == 0 lower triangle for similarity matrix
                if k >0 top k similarities for user u
     */
    def similarities(userVRatings: Map[Int,Map[(Int, Int), Double]], user: Int, choice: String, k : Int = 0) = {


      // user of interest preprocessed ratings
      // userVs similarities :if k == 0 lower triangle for similarity matrix
      //                if k >0 top k similarities for user u
      val userMapping = if (k>0) (1 to 943).toList.filter(_!=user) else (1 to (user -1 )).toList
      val similarities = {
      // iterate over users
        userMapping.map{

          case userV =>

           val userURating = userVRatings.get(user).getOrElse(List()).toMap
           // get preprocessed ratings for given user
             val userVRating = userVRatings.get(userV).getOrElse(List()).toMap
           // compute item intersection between user u and v
             val items = itemIntersection(userVRating.getItems, userURating.getItems)
           // update similarity size of intersection between u and v
             Similarities.updateItems(items.size.toDouble)

             val simis = {
               choice match {
                   // if metric is jacquard
                 case _ if choice == "Jaccard" =>
                   val numerator = items.size
                   val denominator = userURating.getItems.size + userVRating.getItems.size - numerator
                   if (denominator != 0.0) 1.0 * numerator / denominator else 0.0
                  // if metric is adjusted cosine
                 case _ if choice == "AdjustedCosine" => items.map(v => {
                   userURating.get(user, v).getOrElse(0.0) * userVRating.get(userV, v).getOrElse(0.0)
                 }).sum
               }
             }
           // return tuple with user u, v and similarity value
           (user, userV) -> simis

      }
      }
      // if k >0, return top k users in descending
      val sortedSimilarities =  if (k == 0 ) similarities else similarities.sortBy(_._2)(Ordering.Double.reverse).take(k)

      // check computing similarities for bottom triangle of matrix if k == 0
      if (k==0){
        assert (sortedSimilarities.size == user-1)
      }
        // convert to Map
        sortedSimilarities.toMap
    }

    /*
    Compute similarity measurements for all users
    @param  : preprocessed ratings , number of users, choice of metric, number of neighbors
    return for k ==0 the lower triangle matrix of similarity=: Map((Int,Int),Double)
    k>0 : top k similarities per user
     */
    def allUsersSimilarities(data: Map[(Int, Int), Double], numbUsers: Int, choice: String, k: Int=0) = {

      // preprocessed ratings grouped by users
      val userVRatings = data.groupBy(_._1._1)
      // reset similarity item size
      Similarities.similarityItems = List.empty[Double]
      // reset similarity time measurement
      Timer.simiTime = 0.0
      // start time similarity time measurement
      val start_2 = System.nanoTime()
      // compute similarity for all users
      val x = (1 to numbUsers).map(similarities(userVRatings, _, choice,k))
      // end similarity measurement
      val end_2 = System.nanoTime()
      // update similarity measurement with new value
      Timer.updateSimiTime(end_2-start_2)
      // merge similarity of each user
      val mergedSimis = x.reduce(_ ++ _)
      // find total number of non zero s_u,v:
      Similarities.nonZeroSimi =mergedSimis.count(_._2 != 0.0)

      mergedSimis.map{case ((userU,userV), simi) => ((userU,userV),simi)}
    }

/* Compute mean absolute absolute error
@param : rdd of user, item, rating, prediction
return : mean absolute error between rating and prediction : Double
 */
    def mae(predictions: RDD[(Int, Int, Double, Double)]) = {
      predictions.map(a => scala.math.abs(a._3 - a._4)).reduce(_ + _) / predictions.count
    }

/* Compute user specific weighted sum deviation
@param item intersection list, user similarities, preprocessed ratings, user rating, and k neighbors
return user specific weigthed deviation
 */
    def userSpecificWeightedSum (intersection : List[Int], simis: Map[(Int, Int), Double],
      userRating: Map[(Int, Int), Double],rating: Rating , k : Int ) = {

      // if k ==0  map over intersection with user rating given item
      // if k >0  map over user rating given item
        val userWeight= intersection.map {
        v => {
          // user order depends on execution mode
          // if k ==0, search the lower triangle of similarities
          // if k>0, map over user intersection
          val (user1, user2) =  if ( k>0 || (k==0 && rating.user > v)) (rating.user, v) else (v, rating.user)

          (userRating(v,rating.item) * simis(user1, user2), scala.math.abs(simis(user1, user2)))
        }
      }.reduce((a, b) => (a._1 + b._1, a._2 + b._2))

      // edge case make sure denominator is not null
      if (userWeight._2 != 0) (rating.user, (rating.item, rating.rating, userWeight._1 / userWeight._2))
        // replace with zeeo
      else (rating.user, (rating.item, rating.rating, 0.0))
    }

/* Compute prediction with user specific weighted average deviation
@param : train, test sets, k neighbors default to zero; metric default to AdjustedCosine
return prediction : RDD[(Int,Int,Double,Double)]
 */
    def predictor(train: RDD[Rating], test: RDD[Rating], k: Int=0 ,choice: String="AdjustedCosine") = {
      // get all users in test set
      val Users = train.map(_.user).distinct().count().toInt
      // get user average for training set
      val averageUserRatings = train.userRating.featureAverage
      // get train global average if test item not present in train set
      val globalAverage = train.ratingAvg
      // compute preprocessed ratings from training set
      val preprocessedRatings = preprocessRating(train).map(v => (v._1, v._2._1))
      // compute normalized ratings
      val preprocessedRatingsNormalized = preprocessRating(train).map(v => (v._1, v._2._2))
      // compute similarities for train set
      val similarities = allUsersSimilarities(preprocessedRatingsNormalized, Users, choice,k)
      // group similarities by user for k==0 case
      val similaritiesGrouped = similarities.groupBy(_._1._1)

      val itemRatings = preprocessedRatings.groupBy(_._1._2) // group by items


       test
        .map {
          rating =>
            // get all userV ratings for item
            val userRating = itemRatings.get(rating.item).getOrElse(List()).toMap
            // check users have rated the item
            if (userRating.nonEmpty) {
            // case with knn
              if (k>0){
              // get similarities for user u
              val simis  = similaritiesGrouped(rating.user)
              // compute intersection between user rating items and top k users
              val intersection = userRating.keySet.map(_._1).intersect(simis.keySet.map(_._2)).toList
              // make sure intersection is not empty
              if (intersection.nonEmpty) {
                // computed user specific user weighted deviation
                userSpecificWeightedSum(intersection,simis,userRating,rating,k)
                // all userV present in rating and in similarities for userU
              }    else {
                // if no intersection, deviation of zero
                (rating.user, (rating.item, rating.rating, 0.0))
              }
            }else {
                // case without knn
                // get all user v rating item
                val userVs = userRating.map(_._1._1).toList.filter( _ != rating.user)
                // computed user specific user weighted deviation
                userSpecificWeightedSum(userVs,similarities,userRating,rating,k)

            }
              // if no user, item not in training set, only user user average
            }else  (rating.user, (rating.item, rating.rating, 0.0))
        }
         // join with user average
        .leftOuterJoin(averageUserRatings)
         // put -990.0 flag if user not in training set
        .map { case (user, ((item, rating,prediction), userAverage)) =>
          (user, item, rating,prediction, userAverage.getOrElse(-999.0))
        }
        .map {
              // compute prediction, if flag value encountered, replace with global average
          case (user, item, rating, userItemWeight,userAvg) => if (userAvg == -999.0 ) (user, item, rating, globalAverage)
          else (user, item, rating, userAvg + userItemWeight * scale(userAvg + userItemWeight, userAvg))

        }
    }
    val cosinePrediction = predictor(train, test)
    // compute mae for cosine prediction
    // time predictor
    val timePredictionSimilarities =Timer.timer(predictor(train, test).collect())
    // compute cosine prediction
    val maeCosinePredictor = mae(cosinePrediction)
    // compute jacquard prediction
    val jacquardPrediction = predictor(train, test,0, "Jaccard")
    // compute mae for jacquard prediction
    val maeJacquardPredictor = mae(jacquardPrediction)

    val users = train.map(_.user).distinct().count.toInt


    conf.json.toOption match {
      case None => ;
      case Some(jsonFile) => {
        var json = "";
        {
          // Limiting the scope of implicit formats with {}
          implicit val formats = org.json4s.DefaultFormats
          val answers: Map[String, Any] = Map(
            "Q2.3.1" -> Map(
              "CosineBasedMae" -> maeCosinePredictor, // Datatype of answer: Double
              "CosineMinusBaselineDifference" -> (maeJacquardPredictor-0.7669)  // Datatype of answer: Double
            ),

            "Q2.3.2" -> Map(
              "JaccardMae" -> maeJacquardPredictor, // Datatype of answer: Double
              "JaccardMinusCosineDifference" -> (maeJacquardPredictor-maeCosinePredictor)// Datatype of answer: Double
            ),

             "Q2.3.3" -> Map(
               // Provide the formula that computes the number of similarity computations
               // as a function of U in the report.
               "NumberOfSimilarityComputationsForU1BaseDataset" -> computationsUV(users) // Datatype of answer: Int
             ),

                         "Q2.3.4" -> Map(
                           "CosineSimilarityStatistics" -> Map(
                             "min" -> Similarities.similarityItems.min, // Datatype of answer: Double
                             "max" -> Similarities.similarityItems.max, // Datatype of answer: Double
                             "average" -> Similarities.similarityItems.avg, // Datatype of answer: Double
                             "stddev" -> Similarities.similarityItems.stdev // Datatype of answer: Double
                           )
                         ),


                         "Q2.3.5" -> Map(
                           // Provide the formula that computes the amount of memory for storing all S(u,v)
                           // as a function of U in the report.
                           "TotalBytesToStoreNonZeroSimilarityComputationsForU1BaseDataset" -> Similarities.nonZeroSimi * 8 // Datatype of answer: Int
                         ),

                         "Q2.3.6" -> Map(
                           "DurationInMicrosecForComputingPredictions" -> Map(
                             "min" -> timePredictionSimilarities._1.min, // Datatype of answer: Double
                             "max" -> timePredictionSimilarities._1.max, // Datatype of answer: Double
                             "average" -> timePredictionSimilarities._1.avg, // Datatype of answer: Double
                             "stddev" -> timePredictionSimilarities._1.stdev // Datatype of answer: Double
                           )
                           // Discuss about the time difference between the similarity method and the methods
                           // from milestone 1 in the report.
                         ),

                         "Q2.3.7" -> Map(
                           "DurationInMicrosecForComputingSimilarities" -> Map(
                             "min" -> timePredictionSimilarities._2.min, // Datatype of answer: Double
                             "max" -> timePredictionSimilarities._2.max, // Datatype of answer: Double
                             "average" -> timePredictionSimilarities._2.avg, // Datatype of answer: Double
                             "stddev" -> timePredictionSimilarities._2.stdev  // Datatype of answer: Double
                           ),
                           "AverageTimeInMicrosecPerSuv" -> timePredictionSimilarities._2.avg/computationsUV(users), // Datatype of answer: Double
                           "RatioBetweenTimeToComputeSimilarityOverTimeToPredict" -> timePredictionSimilarities._2.avg/timePredictionSimilarities._1.avg // Datatype of answer: Double
                         )

          )
          json = Serialization.writePretty(answers)
        }

        println(json)
        println("Saving answers in: " + jsonFile)
        printToFile(json, jsonFile)
      }
    }


    println("")
    spark.close()
  }




