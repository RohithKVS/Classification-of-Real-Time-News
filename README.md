# Classification-of-Real-Time-News
Instructions:
I. Collecting the offline data:
1. Uncomment the following lines
t=open("train.txt","w")
t.write(json.dumps(story.encode("ascii","ignore")))
t.write('\n')
2. Run the script once with different from and to dates. There is a limit of 200 
news articles for each execution. The more the data, more is the accuracy of the model.
3. Change the line t=open("train.txt","w") to t=open("train.txt","a") to append the
data. Re run the script multiple times with different from and to dates to get more
than 200 news articles as offline data.
II. Run the zookeeper server:
1. Open a terminal. Navigate to kafka directory.
2. Run the following command to run zookeeper server.
bin/zookeeper-server-start.sh config/zookeeper.properties
III. Run the Kafka server:
1. Open a terminal. Navigate to kafka directory.
2. Run the following command to run kafka server.
bin/kafka-server-start.sh config/server.properties
IV. Run the program:
1. Open a terminal
2. Run the command to open spark shell with Kafka integration.
spark-shell --packages org.apache.spark:spark-streaming-kafka-0-10_2.11:2.4.0
3. Change the input directory of the training data text file in Assignment 3.scala.
4. Copy the lines of code till ssc.start() from the Assignment 3.scala and paste in the
terminal.
5. Comment the lines which we uncommented in the stream_producer.py.
6. Open a new terminal.
7. Navigate to the directory which contains the Python file.
8. Run the script to produce the streaming data.
9. After running the python script, run ssc.start() in the first terminal. This predicts
the incoming news articles using the trained model. The output will be a table with
the news articles and the predicted label
10. If there are more than 4 consecutive empty tables, it means all the incoming news articles has been predicted. Run ssc.stop() to stop the Streaming Context.
11. Copy and paste the last two lines of Assignment 3.scala to get the accuracy of the predicted model.
