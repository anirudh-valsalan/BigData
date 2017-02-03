
==========================================================================================================
ReadMe file

I have created a folder named movie inside spark bin folder and copied all input files to that folder.

I have used spark scala shell script for running code for question 1,2 and 3.

Question1
=============
Execute the program in MovieRatingsKmean.scala file in scala spark shell interpreter.

val totalClusters = 10
val totalIterations = 20

output
Cluster Id 0  (2277,Somewhere in the City (1997) , Drama)(3395,Nadine (1987) , Comedy)(1601,Hoodlum (1997) , Crime|Drama|Film-Noir)(178,Love & Human Remains (1993) , Comedy)(556,War Room, The (1993) , Documentary)
Cluster Id 1  (541,Blade Runner (1982) , Film-Noir|Sci-Fi)(1127,Abyss, The (1989) , Action|Adventure|Sci-Fi|Thriller)(1196,Star Wars: Episode V - The Empire Strikes Back (1980) , Action|Adventure|Drama|Sci-Fi|War)
Cluster Id 2  (1994,Poltergeist (1982) , Horror|Thriller)
Cluster Id 3  (1375,Star Trek III: The Search for Spock (1984) , Action|Adventure|Sci-Fi)(1391,Mars Attacks! (1996) , Action|Comedy|Sci-Fi|War)(1377,Batman Returns (1992) , Action|Adventure|Comedy|Crime)(552,Three Musketeers, The (1993) , Action|Adventure|Comedy)(2194,Untouchables, The (1987) , Action|Crime|Drama)
Cluster Id 4  (1962,Driving Miss Daisy (1989) , Drama)(3755,Perfect Storm, The (2000) , Action|Adventure|Thriller)(1968,Breakfast Club, The (1985) , Comedy|Drama)(2420,Karate Kid, The (1984) , Drama)(293,Professional, The (a.k.a. Leon: The Professional) (1994) , Crime|Drama|Romance|Thriller)
Cluster Id 5  (1288,This Is Spinal Tap (1984) , Comedy|Drama|Musical)(296,Pulp Fiction (1994) , Crime|Drama)(2997,Being John Malkovich (1999) , Comedy)(1394,Raising Arizona (1987) , Comedy)
Cluster Id 6  (1197,Princess Bride, The (1987) , Action|Adventure|Comedy|Romance)(2987,Who Framed Roger Rabbit? (1988) , Adventure|Animation|Film-Noir)(2858,American Beauty (1999) , Comedy|Drama)
Cluster Id 7  (1175,Delicatessen (1991) , Comedy|Sci-Fi)(1199,Brazil (1985) , Sci-Fi)(1238,Local Hero (1983) , Comedy)(3814,Love and Death (1975) , Comedy)(1179,Grifters, The (1990) , Crime|Drama|Film-Noir)
Cluster Id 8  (527,Schindler's List (1993) , Drama|War)(457,Fugitive, The (1993) , Action|Thriller)(50,Usual Suspects, The (1995) , Crime|Thriller)(1617,L.A. Confidential (1997) , Crime|Film-Noir|Mystery|Thriller)(1198,Raiders of the Lost Ark (1981) , Action|Adventure)
Cluster Id 9  (2804,Christmas Story, A (1983) , Comedy|Drama)



Question 2
================
a)Exexute the program NaiveBayesGlassClassification.scala file in scala spark shell interpreter
lamda value =1

The accuracy of Naive Bayes Model :87.209

b)Exexute the program DecisionTreeGlassClassification.scala file in scala spark shell interpreter

val numClasses = 8
val categoricalFeaturesInfo = Map[Int, Int]()
val impurity = "gini"
val maxDepth = 5
val maxBins = 100

The accuracy of Decision Tree Model :93.023

Question 3
===================

Exexute the program ALS.scala file in scala spark shell interpreter
I evaluate the recommendation model by measuring the Mean Squared Error of rating prediction.

val rank = 3
val numIterations = 10
Mean Square Error     :0.795





Question 4
================
Kafka Streaming 




Procedure to start the Producer and Consumer Using kafka-console-consumer
============================================================================
First you need to start Zookeeper server. To start it, execute below command:
<kafka_dir>\bin\windows\zookeeper-server-start.bat ..\..\config\zookeeper.properties

Now open another command prompt and start Kafka server:
<kafka_dir>\bin\windows\kafka-server-start.bat ..\..\config\server.properties	

Create Topic

Now you need to create topic to publish and subscribe messages. To create topic you just need to execute below command. As per below command you will be creating topic 'mytopic' with single partition.
<kafka_dir>\bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic mytopic


Produce and Consume Messages

<kafka_dir>\bin\windows\kafka-console-producer.bat --broker-list localhost:9092 --topic mytopic

Now open another command prompt and execute below command. This command prompt will be treated as consumer.

<kafka_dir>\bin\windows\kafka-console-consumer.bat --zookeeper localhost:2181 --topic mytopic


kafka-console-producer.bat --broker-list localhost:9092 --topic mytopicBigramCount
kafka-console-consumer.bat --zookeeper localhost:2181 --topic mytopicBigramCount

For Running Application
==========================

I have used Intellij IDEA and sbt for running this application.Had to modify the porter stemmer jar to support
serialization.



Sample Output
==================
input :Alice is testing spark application. Testing spark is fun.<html><html> hard work hard work.
  
output will be 
(hard work,2)
(test spark,2)