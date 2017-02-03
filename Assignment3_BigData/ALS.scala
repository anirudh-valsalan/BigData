import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating

// Load and parse the data
val data = sc.textFile("movie/ratings.dat")
val ratings = data.map(_.split("::") match { case Array(user, item, rate,timestamp) =>
  Rating(user.toInt, item.toInt, rate.toDouble)
})


val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
val trainingData = splits(0)
val testData = splits(1)
val trainRatings = trainingData.map(_.split("::") match { case Array(user, item, rate,timestamp) =>
  Rating(user.toInt, item.toInt, rate.toDouble)
})
val testRatings=testData.map(_.split("::") match { case Array(user, item, rate,timestamp) =>
  Rating(user.toInt, item.toInt, rate.toDouble)
})

// Build the recommendation model using ALS
val rank = 3
val numIterations = 10
val modelTrain = ALS.train(trainRatings, rank, numIterations, 0.01)

// Evaluate the model on rating data
val usersProducts = testRatings.map { case Rating(user, product, rate) =>
  (user, product)
}
val predictions =
  modelTrain.predict(usersProducts).map { case Rating(user, product, rate) =>
    ((user, product), rate)
  }
val ratesAndPreds = ratings.map { case Rating(user, product, rate) =>
  ((user, product), rate)
}.join(predictions)
val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
  val err = (r1 - r2)
  err * err
}.mean()

println("Mean Squared Error = " + MSE+" in percentage "+(MSE*100)+"%")

//val accuracy =  100.0 *(1-MSE)
//println("Accuracy = " + accuracy)
