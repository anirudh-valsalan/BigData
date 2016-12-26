import java.sql.Date
import java.text.SimpleDateFormat
import org.apache.spark.sql.SQLContext

val currentDate = new Date(System.currentTimeMillis)

def MaxAgeCalculation(xs: Iterable[Float]) = xs.max

def calculateAge(dateOfBirth: String) ={
	val date=dateOfBirth.split("/")
	val presentMonth = currentDate.getMonth()+1
	val presentYear =  currentDate.getYear()+1900
	var presentAge = presentYear - date(2).toInt
	if(date(0).toInt>presentMonth)
	{
		presentAge=presentAge-1
	}
    else if(date(0).toInt==presentMonth){
		val presentDay=currentDate.getDate();
	if(date(1).toInt>presentDay)
	 {
		presentAge=presentAge-1
	 }
	 }
		presentAge.toFloat
}



val friendList = sc.textFile("soc-LiveJournal1Adj.txt")
val userDetails = sc.textFile("userdata.txt")
val friends = friendList.map(line=>line.split("\\t")).filter(line => (line.size == 2)).map(line=>(line(0),line(1).split(",")))
val friendMap=friends.flatMap(x=>x._2.flatMap(z=>Array((z,x._1))))
val userAge = userDetails.map(line=>line.split(",")).map(line=>(line(0),calculateAge(line(9))))
val userAgeAndDetails=friendMap.join(userAge)
val maxAgeValue = userAgeAndDetails.groupBy(_._2._1).mapValues(Max_Age=>MaxAgeCalculation(Max_Age.map(_._2._2))).toArray
val sortedMaxAge = maxAgeValue.sortBy(_._2).reverse
val top10 = sortedMaxAge.take(10)
val top10Parallel= sc.parallelize(top10)
val userDetailsMap = userDetails.map(line=>line.split(","))
val mapperOut= userDetailsMap.map(line=>(line(0),line(1),line(3),line(4),line(5)))
val key = mapperOut.map({case(a,b,c,d,e) => a->(b,c,d,e)})
val joinOut = top10Parallel.join(key).sortBy(_._2._1,false)
val check = joinOut.map(z=>(z._2._2._1,z._2._2._2,z._2._2._3,z._2._2._4,z._2._1)).collect.mkString("\n").replace("(","").replace(")","")
scala.tools.nsc.io.File("final_output4.txt").writeAll(check)
