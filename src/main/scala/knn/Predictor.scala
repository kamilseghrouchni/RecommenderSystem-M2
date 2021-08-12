package knn

import org.rogach.scallop._
import org.json4s.jackson.Serialization
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
import similarity.Rating

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val train = opt[String](required = true)
  val test = opt[String](required = true)
  val json = opt[String]()
  verify()
}

//case class Rating(user: Int, item: Int, rating: Double)

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
  }).coalesce(1)
  assert(train.count == 80000, "Invalid training data")

  println("Loading test data from: " + conf.test())
  val testFile = spark.sparkContext.textFile(conf.test())
  val test = testFile.map(l => {
      val cols = l.split("\t").map(_.trim)
      Rating(cols(0).toInt, cols(1).toInt, cols(2).toDouble)
  }).coalesce(1)
  assert(test.count == 20000, "Invalid test data")

  // Save answers as JSON
  def printToFile(content: String,
                  location: String = "./answers.json") =
    Some(new java.io.PrintWriter(location)).foreach{
      f => try{
        f.write(content)
      } finally{ f.close }
  }

  val users = train.map(_.user).distinct().count().toInt

  /* Compute minimum numbers of bytes for k
  @param : total number of users, and number of neighbors
  return minimum numbers of bytes for k
   */
def computeMinNumberOfBytesForK(user : Int, k : Int) = {

  user*k*8
}
  conf.json.toOption match {
    case None => ;
    case Some(jsonFile) => {
      var json = "";
      {
        // Limiting the scope of implicit formats with {}
        implicit val formats = org.json4s.DefaultFormats


        val answers: Map[String, Any] = Map(
          "Q3.2.1" -> Map(
            // Discuss the impact of varying k on prediction accuracy on
            // the report.
            "MaeForK=10" -> similarity.Predictor.mae(
              similarity.Predictor.predictor(train,test,10)
            ), // Datatype of answer: Double
            "MaeForK=30" -> similarity.Predictor.mae(
              similarity.Predictor.predictor(train,test,30)
            ), // Datatype of answer: Double
            "MaeForK=50" -> similarity.Predictor.mae(
              similarity.Predictor.predictor(train,test,50)
            ), // Datatype of answer: Double
            "MaeForK=100" -> similarity.Predictor.mae(
              similarity.Predictor.predictor(train,test,100)
            ), // Datatype of answer: Double
            "MaeForK=200" ->similarity.Predictor.mae(
              similarity.Predictor.predictor(train,test,200)
            ), // Datatype of answer: Double
            "MaeForK=300" ->similarity.Predictor.mae(
              similarity.Predictor.predictor(train,test,300)
            ), // Datatype of answer: Double
            "MaeForK=400" -> similarity.Predictor.mae(
              similarity.Predictor.predictor(train,test,400)
            ), // Datatype of answer: Double
            "MaeForK=800" -> similarity.Predictor.mae(
              similarity.Predictor.predictor(train,test,800)
            ), // Datatype of answer: Double
            "MaeForK=943" -> similarity.Predictor.mae(
              similarity.Predictor.predictor(train,test,943)
            ), // Datatype of answer: Double
            "LowestKWithBetterMaeThanBaseline" -> 100, // Datatype of answer: Int
            "LowestKMaeMinusBaselineMae" -> (similarity.Predictor.mae(
              similarity.Predictor.predictor(train,test,100))- 0.7669) // Datatype of answer: Double
          ),

          "Q3.2.2" ->  Map(
            // Provide the formula the computes the minimum number of bytes required,
            // as a function of the size U in the report.
            "MinNumberOfBytesForK=10" -> computeMinNumberOfBytesForK(users,10), // Datatype of answer: Int
            "MinNumberOfBytesForK=30" -> computeMinNumberOfBytesForK(users,30), // Datatype of answer: Int
            "MinNumberOfBytesForK=50" -> computeMinNumberOfBytesForK(users,50), // Datatype of answer: Int
            "MinNumberOfBytesForK=100" -> computeMinNumberOfBytesForK(users,100), // Datatype of answer: Int
            "MinNumberOfBytesForK=200" -> computeMinNumberOfBytesForK(users,200), // Datatype of answer: Int
            "MinNumberOfBytesForK=300" -> computeMinNumberOfBytesForK(users,300), // Datatype of answer: Int
            "MinNumberOfBytesForK=400" -> computeMinNumberOfBytesForK(users,400), // Datatype of answer: Int
            "MinNumberOfBytesForK=800" -> computeMinNumberOfBytesForK(users,800), // Datatype of answer: Int
            "MinNumberOfBytesForK=943" -> computeMinNumberOfBytesForK(users,942)  // Datatype of answer: Int, added correction from forum
          ),

          "Q3.2.3" -> Map(
            "SizeOfRamInBytes" -> 8*scala.math.pow(2,30), // Datatype of answer: Int
            "MaximumNumberOfUsersThatCanFitInRam" ->  ((8*scala.math.pow(2,30)) / (8*3*100)).toInt // Datatype of answer: Int
          )

          // Answer the Question 3.2.4 exclusively on the report.
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
