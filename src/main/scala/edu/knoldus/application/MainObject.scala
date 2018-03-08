package edu.knoldus.application

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object MainObject {
  def main(args: Array[String]): Unit = {
    val ssc = new StreamingContext("local[*]", "PrintTweets", Seconds(Constants.INTERVAL))

    val tweets = TwitterUtils.createStream(ssc, None, Array("barcelona"))
    val statuses: DStream[String] = tweets.map(status => status.getText)
    statuses.print()

    val hashTags = tweets.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

    val topCountsTen = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(Constants.WINDOW))
      .map { case (topic, count) => (count, topic) }
      .transform(_.sortByKey(false))

    topCountsTen.foreachRDD(rdd => {
      val topList = rdd.take(3)
      topList.foreach { case (count, tag) => DatabaseHandler.insertInDataBase(tag, count) }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
