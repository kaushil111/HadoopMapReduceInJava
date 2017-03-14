Running the code on AWS Hadoop AMI:

1.	Start the Hadoop AMI
2.	Set up the SSH-AGENT as in the AMI.docx file
3.	Set up the Hadoop configurations and Public IP of all the Nodes as in the AMI.docx 
file
4.	Set up the Hostname of every node as in the AMI.docx file
5.	Start the Hadoop services as in the AMI.docx file
6.	Check the health of the Nodes as in the AMI.docx file
7.	Upload the states.tar and WordCount.jar with the help f FileZilla as in AMI.docx file
8.	Untar the states.tar with the help of following command:

tar ?xvf states.tar

9.	Copy the states folder that contain all the files to HDFS by following command:

hadoop fs -copyFromLocal states hdfs://ec2-xx-xx-xx-xx.us-west-
2.compute.amazonaws.com:8020/states

10.	Put the WordCount.jar files in HDFS
Use the following command:

	hadoop fs -put WordCount.jar hdfs://ec2-xx-xx-xx-xx.us-west-
2.compute.amazonaws.com:8020/user/ubuntu/WordCount.jar
 
11.	 Clear the Output Directory in which the Output of the Map-Reduce is to be expected
12.	Run the Map-Reduce Jobs with the help of following command:

hadoop jar WordCount.jar WordCount hdfs://ec2-xx.xx.xx.xx.us-west-
2.compute.amazonaws.com:8020/states/ hdfs://ec2-52-40-241-30.us-west-
2.compute.amazonaws.com:8020/ output

13.	The last two parameters in the command above means Input and Output directories 
as the input is the states directory
14.	Open the browser
15.	Go to the Health Status page as in the AMI.docx file
16.	Click on utilities and browser the file system
17.	Go to the output directory
18.	Output-3 directory contains the output of problem statement 1.
19.	In the output directory download the part-0000 file that contains the actual result of 
stage 1.
20.	Go back to the previous directory
21.	Output-5 directory contains the output of problem statement 2.
22.	In the output directory download the part-0000 file that contains the actual result of 
stage 5.

