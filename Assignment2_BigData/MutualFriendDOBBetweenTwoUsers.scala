val friendList = sc.textFile("soc-LiveJournal1Adj.txt")
val user1 = readLine("Enter UserA Id : ")
val user2 = readLine("Enter UserB Id : ")
var friend1List=friendList.map(line=>line.split("\\t")).filter(line => (line.size == 2)).filter(a=>(user1==a(0))).flatMap(li=>li(1).split(","))
var friend2List=friendList.map(line=>line.split("\\t")).filter(line => (line.size == 2)).filter(a=>(user2==a(0))).flatMap(li=>li(1).split(","))
val mutualFrinds = friend1List.intersection(friend2List).collect()


val userData = sc.textFile("userdata.txt")

val userDetails = userData.map(line=>line.split(",")).filter(line=>mutualFrinds.contains(line(0))).map(line=>(line(1)+":"+line(9)))
val check=user1+","+user2+"\t["+userDetails.collect().mkString(",")+"]"
scala.tools.nsc.io.File("Output3_Sample.txt").writeAll(check)

 