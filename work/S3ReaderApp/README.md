# S3 Reader APP

https://docs.microsoft.com/en-us/azure/hdinsight/spark/apache-spark-create-standalone-application#prerequisites

Created with Intellij, Scala and Azure Plugin for Intellij

# Setup Guide
1. Clone repo

2. maven clean install

3. run a sample app to verify the setup is successful 

Example running `LogQuery.scala`

```
22/01/17 20:02:19 INFO SparkContext: Running Spark version 3.0.1
...
22/01/17 20:02:23 INFO DAGScheduler: Job 0 finished: collect at LogQuery.scala:86, took 0.772280 s
(10.10.10.10,"FRED",GET http://images.com/2013/Generic.jpg HTTP/1.1)	bytes=621	n=2
22/01/17 20:02:23 INFO AbstractConnector: Stopped Spark@5c48c0c0{HTTP/1.1,[http/1.1]}{0.0.0.0:4040}
22/01/17 20:02:23 INFO SparkUI: Stopped Spark web UI at http://Workstation-Rig.mshome.net:4040
22/01/17 20:02:23 INFO BlockManagerInfo: Removed broadcast_1_piece0 on Workstation-Rig.mshome.net:49398 in memory (size: 2.2 KiB, free: 1996.2 MiB)
22/01/17 20:02:23 INFO MapOutputTrackerMasterEndpoint: MapOutputTrackerMasterEndpoint stopped!
...
22/01/17 20:02:23 INFO ShutdownHookManager: Deleting directory C:\Users\stanc\AppData\Local\Temp\spark-10348f37-d3c5-4e1c-bfa3-1f2014c6c4b2
Process finished with exit code 0

```
visit http://localhost:8082/ for the logs
![img.png](img.png)