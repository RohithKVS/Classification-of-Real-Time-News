import org.apache.spark.ml.feature.{Tokenizer,IDF}

import org.apache.spark.ml.feature.StopWordsRemover

import org.apache.spark.ml.feature.HashingTF

import org.apache.spark.ml.{Pipeline, PipelineModel}

import org.apache.spark.ml.classification.NaiveBayes

import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

import org.apache.kafka.clients.consumer.ConsumerConfig

import org.apache.kafka.common.serialization.StringDeserializer

import org.apache.spark.SparkConf

import org.apache.spark.streaming._

import org.apache.spark.streaming.kafka010._

case class news_articles(label: Double,data: String);

val news_data=sc.textFile("/home/rohith/Desktop/train.txt").map(l => {

val line=l.split("""\|\|""");

news_articles(line(0).substring(1,line(0).length).toDouble,line(1))

}).toDF;

val tokenizer = new Tokenizer().setInputCol("data").setOutputCol("words");

val stop_words = new StopWordsRemover().setInputCol(tokenizer.getOutputCol).setOutputCol("stop_filtered");

val hashingTF = new HashingTF().setInputCol(stop_words.getOutputCol).setOutputCol("rawFeatures");

val idf = new IDF().setInputCol(hashingTF.getOutputCol).setOutputCol("features");

val naiveBayes = new NaiveBayes()

val pipeline = new Pipeline().setStages(Array(tokenizer,stop_words, hashingTF, idf,naiveBayes))

val model = pipeline.fit(news_data);

val ssc = new StreamingContext(sc, Seconds(1))

val topics = Array("guardian2")

val kafkaParams = Map[String, Object](
  "bootstrap.servers" -> "localhost:9092",
  "key.deserializer" -> classOf[StringDeserializer],
  "value.deserializer" -> classOf[StringDeserializer],
  "group.id" -> "consumer-1",
  "auto.offset.reset" -> "latest",
  "enable.auto.commit" -> (false: java.lang.Boolean)
)

val stream = KafkaUtils.createDirectStream[String, String](
  ssc,
  LocationStrategies.PreferConsistent,
  ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
)

var accuracy_count=0.0

val res=stream.map(record => (record.value))

res.foreachRDD { rdd =>

  val wordsDataFrame = rdd.map(l=>l.split("""\|\|""")).map(line => news_articles(line(0).toDouble,line(1))).toDF()  

  val predictions=model.transform(wordsDataFrame);

  val evaluator = new MulticlassClassificationEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("accuracy");

  val accuracy = evaluator.evaluate(predictions)

  predictions.show()

  println(s"Accuracy for the news article = $accuracy")
  if(!accuracy.isNaN)
  {
    accuracy_count+=accuracy;
  }
}

ssc.start()

accuracy_count=accuracy_count/200

println(s"Total accuracy=$accuracy_count")