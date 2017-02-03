import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors

val totalClusters = 10
val totalIterations = 20
val itemUserData = sc.textFile("movie/itemusermat")
val dataToClassify = itemUserData.map(c => Vectors.dense(c.split(' ').drop(1).map(_.toDouble))).cache()

val movieInput = sc.textFile("movie/movies.dat")
val  movieData = movieInput.map( line=>	(line.split("::"))).map(m=>(m(0),(m(1)+" , "+m(2))))


val outputClusters = KMeans.train(dataToClassify, totalClusters, totalIterations)

val predictionOut = itemUserData.map{ 
input =>(input.split(' ')(0),
outputClusters.predict(Vectors.dense(input.split(' ').drop(1).map(_.toDouble)))
)
}




val movieJoinOut = predictionOut.join(movieData)
val groupClusters = movieJoinOut.map(clusterMap=>(clusterMap._2._1,(clusterMap._1,clusterMap._2._2))).groupByKey()

val summary = groupClusters.map(x=>(x._1,x._2.toList))

val outputToConsole = summary.map(f=>(f._1,f._2.take(5))).sortByKey().collect()
println("Cluster Id , summary of first 5 Movies in cluster")
outputToConsole.foreach(
clusterInfo=>println("Cluster Id "+clusterInfo._1+" , "+clusterInfo._2.mkString("")

))

val outputTofile=summary.map(p=>(p._1,p._2.take(5))).sortByKey()
val check=outputTofile.map(a => "Cluster Id: "+a._1.toString + "\t" + a._2.map(aa=>aa.toString).toArray.mkString(",")).collect.mkString("\n")
scala.tools.nsc.io.File("movie/final_out1.txt").writeAll(check)


