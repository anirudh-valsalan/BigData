ASSIGNMENT 1

1)
i) File Name: FriendRecommendation.java 
Command to run:
hadoop jar FriendRecommendation.jar FriendRecommendation /socNetData/networkdata output
To see output1:
Hdfs dfs –cat output/*

ii) Recommendations for the users with following user IDs:

924	439,2409,6995,11860,15416,43748,45881
8941	8943,8944,8940
8942	8939,8940,8943,8944
9019	9022,317,9023
9020	9021,9016,9017,9022,317,9023
9021	9020,9016,9017,9022,317,9023
9022	9019,9020,9021,317,9016,9017,9023
9990	13134,13478,13877,34299,34485,34642,37941
9992	9987,9989,35667,9991
9993	9991,13134,13478,13877,34299,34485,34642,37941


2)
File Name: MutualFriends.java
Command to run :
hadoop jar FriendRecommendation.jar MutualFriends <user1> <user2> /socNetData/networkdata commonfriend
commonfriend contains the list of the user id of mutual friends for the given two user ids.
To see output2:
Hdfs dfs –cat commonfriend/*

Output: 
0:1 	5,20
3)
File Name: 
i)	FriendsZipcode1.java
ii)	FriendsZipcode2.java

Command to run:
hadoop jar FriendRecommendation.jar InMemoryJoin <user1> <user2> /socNetData/networkdata/ intermediate_output /socNetData/userdata/userdata.txt nameFriend
nameFriend contains the list of the names and the zipcode of their mutual friends.
To see nameFriend:
Hdfs dfs –cat nameFriend/*
Output for userId 0 and 1: 
0:1     [Beth:33463,Juan:29201]

4)
File Name: reduceSideJoin.java
Command to run:
hadoop jar FriendRecommendation.jar AverageAge /socNetData/userdata/userdata.txt /socNetData/networkdata /socNetData/userdata inter1 averageage
averageage contains User Name, address as given format and average age of all the direct friends of particular User Id.  Top 20 users are sorted by the calculated average age from step 1 in descending order.
To see average:
Hdfs dfs –cat averageage_int2/*


