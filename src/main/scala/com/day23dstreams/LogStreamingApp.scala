package com.day23dstreams

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object LogStreamingApp {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day23-DStreams-Basics")
      .setMaster("local[*]")

    // Create StreamingContext with a 5-second batch interval
    val streamingContext = new StreamingContext(conf, Seconds(5))

    // Read text data from a socket
    val lines = streamingContext.socketTextStream("localhost", 9999)

    // flatMap: split each log line into words
    val words = lines.flatMap(_.split("\\s+"))

    // map: convert each word into (word, 1)
    val wordPairs = words.map(word => (word, 1))

    // filter: keep only ERROR messages
    val errorWords = wordPairs.filter {
      case (word, _) => word == "ERROR"
    }

    // Count ERROR messages in each micro-batch
    val errorCount = errorWords.reduceByKey(_ + _)

    // Display the result
    errorCount.print()

    // Start streaming
    streamingContext.start()

    println("DStreams application started...")
    println("Listening for logs on localhost:9999")
    println("Batch interval: 5 seconds")

    // Keep the application running
    streamingContext.awaitTermination()
  }
}

