ReadMe
========================================================================
1)I have installed the spark cluster on my windows machine
2)Copied the two input files soc-LiveJournal1Adj.txt and userdata.txt to the bin folder inside spark-1.6.0-bin-hadoop2.6/ bin folder
3)Add jar files stanford-corenlp-3.6.0.jar and stanford-corenlp-3.6.0-models.jar to the class path.Jdk 8 is required to run this jar file
4)Execute the lines one by one in the Scala interpreter 


Question 1
==========
FileName :MutualFriend.scala

0,1	5,20
20,28193	1
1, 29826 - no mutual friends
6222,19272	19263,19280,19281,19282
28041,28054	6245,6236,28056,28061

Question 2
==============================
FileName :MutualFriendsBetweenTwoUsers.scala
0,1	5,20

Question 3
==========
FileName :MutualFriendDOBBetweenTwoUsers.scala

0,1     [Beth:8/27/1970,Juan:9/12/1991]
6222,19272      [Willie:5/18/1965,Hilton:6/22/1965,Betty:6/10/1958,Charles:2/2/1986]


Question 4
=============
File Name :MaxAgeFriendWithAddress.scala

Nicole,3820 Sunset Drive,Black Fish Lake,Arkansas,86.0
Tommie,2850 Laurel Lane,Pecos,Texas,86.0
Faith,4034 Joy Lane,Burbank,California,86.0
Dominga,258 Smith Street,Cambridge,Massachusetts,86.0
James,4868 Lincoln Street,Camden,New Jersey,86.0
Amanda,4294 Lamberts Branch Road,Doral,Florida,86.0
Jill,4866 Rainbow Road,Los Angeles,California,86.0
Bradley,2116 Rodney Street,Warrenton,Missouri,86.0
Jennifer,934 Hood Avenue,San Diego,California,86.0
Daniel,858 Trainer Avenue,Varna,Illinois,86.0


Question 5
===============
File Name:Bigram.scala

To add jar files use the following commands
:cp stanford-corenlp-3.6.0-models.jar
:cp stanford-corenlp-3.6.0.jar
(I have removed the jar files to reduce the folder size)
input line:Alice is testing spark application. Testing spark is fun
Map((test,spark) -> 2)


I have included  Bigram.py which is python code for Bigram finding.It uses nltk jar files for stemming.
I have used databriks to run this application and used amazon web services s3 cluster to load the input files.


