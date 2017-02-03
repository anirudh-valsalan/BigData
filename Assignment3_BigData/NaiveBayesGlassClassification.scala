import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

val glassData = sc.textFile("movie/glass.data")
val labelFeatures = glassData.map { line =>
  val features = line.split(',')
  LabeledPoint(features(10).toDouble, Vectors.dense(features(0).toDouble,features(1).toDouble,features(2).toDouble,features(3).toDouble,features(4).toDouble,features(5).toDouble,features(6).toDouble,features(7).toDouble,features(8).toDouble,features(9).toDouble))
}

val trainTestSplit = labelFeatures.randomSplit(Array(0.6, 0.4), seed = 11L)
val trainingData = trainTestSplit(0)
val testingData = trainTestSplit(1)

val naiveModel = NaiveBayes.train(trainingData, lambda = 1.0)

val predictionNaiveBayes = testingData.map(p => (naiveModel.predict(p.features), p.label))
val successCount =predictionNaiveBayes.filter(x => x._1 == x._2).count()
val modelAccuracy = 100.0 * successCount.toDouble / testingData.count()
println("Accuracy of Naive Bayes is= " + modelAccuracy+"%")