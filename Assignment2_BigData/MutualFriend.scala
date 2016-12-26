def createPair(a: Array[String]): Array[((String, String), Array[String])] = {
    
		  val firstTerm=a(0)
                  val secondTermAsList=a(1).split(",")
		  secondTermAsList.map{b=>
			val key=if(b.toInt>firstTerm.toInt) (firstTerm,b) else (b,firstTerm)
			val value=secondTermAsList
			(key,value)
          }
		 
}
val inputRDD= sc.textFile("soc-LiveJournal1Adj.txt")
val out =inputRDD.map(line=>line.split("\\t")).filter(line => (line.size == 2)).flatMap(createPair)

var z=out.reduceByKey((a,b) =>a.intersect(b)).filter(a => a._2.nonEmpty)
val check=z.map(a => a._1.toString + "\t" + a._2.map(aa=>aa.toString).toArray.mkString(",")).collect.mkString("\n").replace("(","").replace(")","")
scala.tools.nsc.io.File("final_out1.txt").writeAll(check)
