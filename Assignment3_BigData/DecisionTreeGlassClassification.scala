import org.apache.spark.SparkContext
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.impurity.Gini



val impurity = "gini"
val maxDepth = 5
val maxBins = 100
val numClassesReq = 8
val categoricalFeaturesInfo = Map[Int, Int]()

val glassData = sc.textFile("movie/glass.data")
val labelFeatures = glassData.map { line =>
  val features = line.split(',')
  LabeledPoint(features(10).toDouble, Vectors.dense(features(0).toDouble,features(1).toDouble,features(2).toDouble,features(3).toDouble,features(4).toDouble,features(5).toDouble,features(6).toDouble,features(7).toDouble,features(8).toDouble,features(9).toDouble))
}

val splits = labelFeatures.randomSplit(Array(0.6, 0.4), seed = 11L)
val trainingData = splits(0)
val testingData = splits(1)

val model = DecisionTree.trainClassifier(trainingData, numClassesReq, categoricalFeaturesInfo,impurity, maxDepth, maxBins)

val labelAndPreds = testingData.map { points =>
val prediction = model.predict(points.features)
(points.label,prediction)
}
val successCount =labelAndPreds.filter(r => r._1 == r._2).count
val accuracy =  100.0 *successCount.toDouble / testingData.count
println("Accuracy of Decision Tree is= " + accuracy+"%")